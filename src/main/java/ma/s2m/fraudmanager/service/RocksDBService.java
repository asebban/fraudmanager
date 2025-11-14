package ma.s2m.fraudmanager.service;

import ma.s2m.fraudmanager.model.Measurment;
import ma.s2m.fraudmanager.util.RetryUtil;
import ma.s2m.serializer.SerializationManager;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Version RocksDB avec :
 * - écriture ASYNCHRONE
 * - sérialisation déportée dans le writer
 * - usage de SerializationManager pour les blobs écrits
 */
public class RocksDBService {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBService.class);

    private static final int LOCK_TIMEOUT_SECONDS = 30;
    private static final String LOCK_PREFIX = "lock:";

    // on garde Jackson pour les lectures Map<Long,Measurment> (si tu veux je te fais aussi en binaire)
    private final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<Map<Long, Measurment>> MEAS_MAP_TYPE =
            new TypeReference<Map<Long, Measurment>>() {};

    private final RocksDB db;
    private final AsyncRocksDbWriter writer;

    public RocksDBService(String dbPath, Integer queueSize) {
        this.writer = AsyncRocksDbWriter.getInstance(dbPath, queueSize);
        this.db = writer.db();
    }

    /**
     * Lecture synchrone d'une map de windows (comme avant)
     */
    public Map<Long, Measurment> getMeasurments(String key) {
        return RetryUtil.retry(() -> {
            try {
                byte[] value = db.get(toBytes(key));
                if (value == null) {
                    logger.debug("Key {} not found, returning empty map", key);
                    return new HashMap<>();
                }
                // lecture JSON comme avant
                Map<Long, Measurment> measurments = mapper.readValue(value, MEAS_MAP_TYPE);
                logger.debug("Read {} windows of measurments from key: {}", measurments.size(), key);
                return measurments;
            } catch (Exception e) {
                logger.error("Error while getting measurments for key: {}", key, e);
                return new HashMap<>();
            }
        });
    }

    /**
     * Écriture ASYNC d'une map de windows.
     * ⚠️ ICI on ne sérialise plus -> on envoie l'objet au writer.
     */
    public void setMeasurments(String key, Map<Long, Measurment> measurments) {
        RetryUtil.retry(() -> {
            byte[] k = toBytes(key);
            boolean offered = writer.submitPutObject(k, measurments);
            if (!offered) {
                logger.warn("Async save queue is full, measurments for key {} NOT enqueued", key);
            } else {
                logger.debug("Enqueued {} measurments to key: {} (deferred serialization)", measurments.size(), key);
            }
        });
    }

    /**
     * Écriture ASYNC d'une seule measurment.
     * ⚠️ plus de sérialisation ici
     */
    public void setMeasurmentByKey(String key, Measurment measurment) {
        long updateStart = System.currentTimeMillis();
        RetryUtil.retry(() -> {
            byte[] k = toBytes(key);
            boolean offered = writer.submitPutObject(k, measurment);
            if (!offered) {
                logger.warn("Async save queue is full, Measurment for key {} NOT enqueued", key);
            } else {
                logger.debug("Enqueued Measurment to key: {} (deferred serialization)", key);
            }
        });
        long updateEnd = System.currentTimeMillis();
        logger.debug("Time {} : setMeasurmentByKey for key {}", (updateEnd - updateStart), key);
    }

    /**
     * Lock "best effort" en RocksDB.
     */
    public boolean acquireLock(String lockKey, String lockValue) {
        return true;
        /*String realKey = LOCK_PREFIX + lockKey;
        byte[] k = toBytes(realKey);

        try {
            byte[] existing = db.get(k);
            if (existing == null) {
                boolean ok = writer.submitPut(k, toBytes(lockValue)); // ici put direct binaire
                if (!ok) {
                    logger.warn("Lock queue full, cannot acquire lock for key {}", realKey);
                    return false;
                }
                logger.debug("Lock acquired for key: {}", realKey);
                return true;
            } else {
                String currentVal = new String(existing, StandardCharsets.UTF_8);
                boolean acquired = currentVal.equals(lockValue);
                if (acquired) {
                    logger.debug("Lock already owned for key: {}", realKey);
                }
                return acquired;
            }
        } catch (Exception e) {
            logger.error("Error acquiring lock for key: {}", realKey, e);
            return false;
        }*/
    }

    public boolean releaseLock(String lockKey, String lockValue) {
        return true;
        /*String realKey = LOCK_PREFIX + lockKey;
        byte[] k = toBytes(realKey);

        try {
            byte[] existing = db.get(k);
            if (existing == null) {
                return true;
            }
            String currentVal = new String(existing, StandardCharsets.UTF_8);
            if (!currentVal.equals(lockValue)) {
                logger.warn("Failed to release lock for key: {} (not owned by this instance)", realKey);
                return false;
            }
            boolean ok = writer.submitDelete(k);
            if (!ok) {
                logger.warn("Lock queue full, cannot release lock {}", realKey);
                return false;
            }
            logger.debug("Lock released for key: {}", realKey);
            return true;
        } catch (Exception e) {
            logger.error("Error releasing lock for key: {}", realKey, e);
            return false;
        }*/
    }

    public void deleteKey(String key) {
        RetryUtil.retry(() -> {
            boolean ok = writer.submitDelete(toBytes(key));
            if (!ok) {
                logger.warn("Async delete queue is full, key {} NOT deleted now", key);
            } else {
                logger.debug("Async delete enqueued for key: {}", key);
            }
        });
    }

    public List<String> getKeysByPattern(String pattern) {
        return RetryUtil.retry(() -> {
            try {
                if (pattern.endsWith("*")) {
                    String prefix = pattern.substring(0, pattern.length() - 1);
                    byte[] prefixBytes = toBytes(prefix);
                    List<String> keys = new ArrayList<>();

                    try (RocksIterator it = db.newIterator()) {
                        for (it.seek(prefixBytes); it.isValid(); it.next()) {
                            byte[] key = it.key();
                            if (!startsWith(key, prefixBytes)) {
                                break;
                            }
                            keys.add(fromBytes(key));
                        }
                    }

                    logger.debug("Found {} keys matching prefix pattern: {}", keys.size(), pattern);
                    return keys;
                } else {
                    byte[] v = db.get(toBytes(pattern));
                    if (v != null) {
                        return List.of(pattern);
                    } else {
                        return List.of();
                    }
                }
            } catch (Exception e) {
                logger.error("Error getting keys for pattern: {}", pattern, e);
                return List.of();
            }
        });
    }

    /**
     * Lecture synchrone d'une seule measurment.
     * Ici tu peux décider : lecture JSON (comme avant) ou lecture binaire via SerializationManager.
     * Je te laisse JSON pour compat.
     */
    public Measurment getMeasurmentByKey(String key) {
        try {
            byte[] v = db.get(toBytes(key));
            if (v != null) {
                // SI tu veux binaire :
                // return SerializationManager.deserialize(v);
                return (Measurment) SerializationManager.deserialize(v);
            }
        } catch (Exception e) {
            logger.error("Error retrieving Measurment from RocksDB for key {}: {}", key, e.getMessage(), e);
        }
        return null;
    }

    public void close() {
        writer.shutdown();
    }

    // ================== utils ==================

    private static byte[] toBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String fromBytes(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) return false;
        }
        return true;
    }

    // ============================================================
    // ===============  WRITER ASYNC DÉPORTÉ ======================
    // ============================================================

    private static class AsyncRocksDbWriter {

        private static final Logger logger = LoggerFactory.getLogger(AsyncRocksDbWriter.class);

        private static volatile AsyncRocksDbWriter instance;

        private final RocksDB db;
        private final BlockingQueue<WriteRequest> queue;
        private final Thread worker;
        private final WriteOptions writeOptions;
        private volatile boolean running = true;

        static {
            RocksDB.loadLibrary();
        }

        @SuppressWarnings("resource")
        private AsyncRocksDbWriter(String dbPath, int queueSize) {
            try {
                Options options = new Options()
                        .setCreateIfMissing(true)
                        .setCompressionType(CompressionType.LZ4_COMPRESSION);
                this.db = RocksDB.open(options, dbPath);
            } catch (RocksDBException e) {
                throw new RuntimeException("Cannot open RocksDB at " + dbPath, e);
            }
            this.queue = new ArrayBlockingQueue<>(queueSize);
            this.writeOptions = new WriteOptions().setSync(false);

            this.worker = new Thread(this::runWorker, "rocksdb-writer");
            this.worker.setDaemon(true);
            this.worker.start();
        }

        public static AsyncRocksDbWriter getInstance(String dbPath, int queueSize) {
            if (instance == null) {
                synchronized (AsyncRocksDbWriter.class) {
                    if (instance == null) {
                        instance = new AsyncRocksDbWriter(dbPath, queueSize);
                    }
                }
            }
            return instance;
        }

        public RocksDB db() {
            return db;
        }

        // put binaire (locks, delete, etc.)
        public boolean submitPut(byte[] key, byte[] value) {
            return queue.offer(WriteRequest.putBinary(key, value));
        }

        // put objet -> sérialisation dans le writer
        public boolean submitPutObject(byte[] key, Object valueObj) {
            logger.debug("[key = {}] rocksDB queue size is {}", key, queue.size());
            return queue.offer(WriteRequest.putObject(key, valueObj));
        }

        public boolean submitDelete(byte[] key) {
            return queue.offer(WriteRequest.del(key));
        }

        private void runWorker() {
            while (running) {
                try {
                    logger.debug("RocksDB queue size before take: {}", queue.size());
                    WriteRequest first = queue.take();
                    try (WriteBatch batch = new WriteBatch()) {
                        apply(batch, first);

                        int drained = 0;
                        while (drained < 200) {
                            WriteRequest next = queue.poll();
                            if (next == null) break;
                            apply(batch, next);
                            drained++;
                        }

                        db.write(writeOptions, batch);
                    }
                } catch (InterruptedException ignored) {
                    // shutdown
                } catch (Exception e) {
                    logger.error("Error in RocksDB writer thread", e);
                }
            }
        }

        private void apply(WriteBatch batch, WriteRequest req) throws RocksDBException {
            switch (req.type) {
                case PUT_BINARY -> batch.put(req.key, req.value);
                case PUT_OBJECT -> {
                    // sérialisation ICI, dans le thread writer
                    byte[] serialized=null;
                    try {
                        serialized = SerializationManager.serialize(req.objectValue);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    batch.put(req.key, serialized);
                }
                case DEL -> batch.delete(req.key);
            }
        }

        public void shutdown() {
            running = false;
            worker.interrupt();
            db.close();
        }

        private static class WriteRequest {
            enum Type { PUT_BINARY, PUT_OBJECT, DEL }

            final Type type;
            final byte[] key;
            final byte[] value;     // pour PUT_BINARY
            final Object objectValue; // pour PUT_OBJECT

            private WriteRequest(Type type, byte[] key, byte[] value, Object objectValue) {
                this.type = type;
                this.key = key;
                this.value = value;
                this.objectValue = objectValue;
            }

            static WriteRequest putBinary(byte[] key, byte[] value) {
                return new WriteRequest(Type.PUT_BINARY, key, value, null);
            }

            static WriteRequest putObject(byte[] key, Object obj) {
                return new WriteRequest(Type.PUT_OBJECT, key, null, obj);
            }

            static WriteRequest del(byte[] key) {
                return new WriteRequest(Type.DEL, key, null, null);
            }
        }
    }
}
