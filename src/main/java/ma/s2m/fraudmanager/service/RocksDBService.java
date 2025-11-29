package ma.s2m.fraudmanager.service;

import ma.medtech.droolbuilder.rules.Subject;
import ma.s2m.fraudmanager.config.AppConfig;
import ma.s2m.fraudmanager.model.Measurment;
import ma.s2m.fraudmanager.model.WrapperMeasurment;
import ma.s2m.fraudmanager.util.RetryUtil;
import org.apache.fury.Fury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.config.Language;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * RocksDB service with asynchronous writes.
 * - Writes are delegated to a background writer thread.
 * - Objects are serialized inside the writer using {@link Fury}.
 * - Uses a column family per subject (CARD, MERCHANT, CUSTOM) and a
 *   prefix‑rich key format: <subject>[:<custom>]:<key>:<windowSize>.
 */
public class RocksDBService {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBService.class);

    // Jackson mapper for legacy JSON maps
    private final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<Map<Long, Measurment>> MEAS_MAP_TYPE =
            new TypeReference<Map<Long, Measurment>>() {};

    // RocksDB handles
    private final RocksDB db;
    private final ColumnFamilyHandle cfDefault;
    private final ColumnFamilyHandle cfCard;
    private final ColumnFamilyHandle cfMerchant;
    private final ColumnFamilyHandle cfCustom;

    // Async writer (sharded)
    private final AsyncRocksDbWriter writer;

    // Thread‑safe Fury for reads
    private final ThreadSafeFury readFury;

    public RocksDBService(String dbPath, int queueSize) {
        // -----------------------------------------------------------------
        // Open RocksDB with column families
        // -----------------------------------------------------------------
        try {
            @SuppressWarnings("resource")
            ColumnFamilyOptions defaultCFOptions = new ColumnFamilyOptions()
                    .setCompactionStyle(CompactionStyle.UNIVERSAL)
                    .setWriteBufferSize(64 * 1024 * 1024) // 64 MiB
                    .setMaxWriteBufferNumber(3)
                    .setTargetFileSizeBase(64 * 1024 * 1024);
            List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultCFOptions),
                    new ColumnFamilyDescriptor(Subject.CARD.getBytes(StandardCharsets.UTF_8), defaultCFOptions),
                    new ColumnFamilyDescriptor(Subject.MERCHANT.getBytes(StandardCharsets.UTF_8), defaultCFOptions),
                    new ColumnFamilyDescriptor(Subject.CUSTOM.getBytes(StandardCharsets.UTF_8), defaultCFOptions)
            );
            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            @SuppressWarnings("resource")
            DBOptions dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    .setIncreaseParallelism(Runtime.getRuntime().availableProcessors());
            this.db = RocksDB.open(dbOptions, dbPath, cfDescriptors, cfHandles);
            this.cfDefault = cfHandles.get(0);
            this.cfCard = cfHandles.get(1);
            this.cfMerchant = cfHandles.get(2);
            this.cfCustom = cfHandles.get(3);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to open RocksDB with column families", e);
        }

        // -----------------------------------------------------------------
        // Async writer – it will reuse the same RocksDB instance
        // -----------------------------------------------------------------
        this.writer = AsyncRocksDbWriter.getInstance(this.db, queueSize);

        // -----------------------------------------------------------------
        // Fury for synchronous reads
        // -----------------------------------------------------------------
        this.readFury = Fury.builder()
                .withLanguage(Language.JAVA)
                .requireClassRegistration(false)
                .buildThreadSafeFury();
    }

    /** Helper to pick the correct column family based on the key prefix */
    private ColumnFamilyHandle getCFHandleForKey(String key) {
        // Expected key format: subject[:custom]:key:windowSize
        int firstSep = key.indexOf(FraudProcessor.KEY_SEPARATOR);
        if (firstSep == -1) {
            return cfDefault;
        }
        String subjectPart = key.substring(0, firstSep);
        switch (subjectPart) {
            case Subject.CARD:
                return cfCard;
            case Subject.MERCHANT:
                return cfMerchant;
            default:
                if (subjectPart.startsWith(Subject.CUSTOM)) {
                    return cfCustom;
                }
                return cfDefault;
        }
    }

    // -----------------------------------------------------------------
    // READ APIs
    // -----------------------------------------------------------------
    public Map<Long, Measurment> getMeasurments(String key) {
        return RetryUtil.retry(() -> {
            try {
                ColumnFamilyHandle cf = getCFHandleForKey(key);
                byte[] value = db.get(cf, toBytes(key));
                if (value == null) {
                    logger.debug("Key {} not found, returning empty map", key);
                    return new HashMap<>();
                }
                return mapper.readValue(value, MEAS_MAP_TYPE);
            } catch (Exception e) {
                logger.error("Error while getting measurments for key: {}", key, e);
                return new HashMap<>();
            }
        });
    }

    public Measurment getMeasurmentByKey(String key) {
        try {
            ColumnFamilyHandle cf = getCFHandleForKey(key);
            byte[] v = db.get(cf, toBytes(key));
            if (v != null) {
                return (Measurment) readFury.deserialize(v);
            }
        } catch (Exception e) {
            logger.error("Error retrieving Measurment from RocksDB for key {}: {}", key, e.getMessage(), e);
        }
        return null;
    }

    public WrapperMeasurment getWrapperMeasurmentByKey(String key) {
        try {
            ColumnFamilyHandle cf = getCFHandleForKey(key);
            byte[] v = db.get(cf, toBytes(key));
            if (v != null) {
                return (WrapperMeasurment) readFury.deserialize(v);
            }
        } catch (Exception e) {
            logger.error("Error retrieving WrapperMeasurment from RocksDB for key {}: {}", key, e.getMessage(), e);
        }
        return null;
    }

    // -----------------------------------------------------------------
    // WRITE APIs – async, column‑family aware
    // -----------------------------------------------------------------
    public void setMeasurments(String key, Map<Long, Measurment> measurments) {
        RetryUtil.retry(() -> {
            ColumnFamilyHandle cf = getCFHandleForKey(key);
            boolean ok = writer.submitPutObjectWithCF(cf, toBytes(key), measurments);
            if (!ok) {
                logger.warn("Failed to persist measurments for key {} (queue full and fallback failed)", key);
            } else {
                logger.debug("Persisted measurments for key {}", key);
            }
        });
    }

    public void setMeasurmentByKey(String key, Measurment measurment) {
        long start = System.currentTimeMillis();
        RetryUtil.retry(() -> {
            ColumnFamilyHandle cf = getCFHandleForKey(key);
            boolean ok = writer.submitPutObjectWithCF(cf, toBytes(key), measurment);
            if (!ok) {
                logger.warn("Failed to persist measurment for key {} (queue full and fallback failed)", key);
            } else {
                logger.debug("Persisted measurment for key {}", key);
            }
        });
        long end = System.currentTimeMillis();
        logger.debug("Time {} ms : setMeasurmentByKey for key {}", (end - start), key);
    }

    public void setWrapperMeasurmentByKey(String key, WrapperMeasurment wrapperMeasurment) {
        long start = System.currentTimeMillis();
        RetryUtil.retry(() -> {
            ColumnFamilyHandle cf = getCFHandleForKey(key);
            boolean ok = writer.submitPutObjectWithCF(cf, toBytes(key), wrapperMeasurment);
            if (!ok) {
                logger.warn("Failed to persist WrapperMeasurment for key {} (queue full and fallback failed)", key);
            } else {
                logger.debug("Persisted WrapperMeasurment for key {}", key);
            }
        });
        long end = System.currentTimeMillis();
        logger.debug("Time {} ms : setWrapperMeasurmentByKey for key {}", (end - start), key);
    }

    // -----------------------------------------------------------------
    // Simple lock stubs (kept for compatibility)
    // -----------------------------------------------------------------
    public boolean acquireLock(String lockKey, String lockValue) { return true; }
    public boolean releaseLock(String lockKey, String lockValue) { return true; }

    // -----------------------------------------------------------------
    // DELETE API (column‑family aware)
    // -----------------------------------------------------------------
    public void deleteKey(String key) {
        RetryUtil.retry(() -> {
            ColumnFamilyHandle cf = getCFHandleForKey(key);
            boolean ok = writer.submitDelete(cf, toBytes(key));
            if (!ok) {
                logger.warn("Async delete queue is full, key {} NOT deleted now", key);
            } else {
                logger.debug("Async delete enqueued for key: {}", key);
            }
        });
    }

    // -----------------------------------------------------------------
    // PATTERN‑MATCHING HELPERS (unchanged – work across all CFs)
    // -----------------------------------------------------------------
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
                            if (!startsWith(key, prefixBytes)) break;
                            keys.add(fromBytes(key));
                        }
                    }
                    logger.debug("Found {} keys matching prefix pattern: {}", keys.size(), pattern);
                    return keys;
                } else {
                    byte[] v = db.get(toBytes(pattern));
                    return (v != null) ? List.of(pattern) : List.of();
                }
            } catch (Exception e) {
                logger.error("Error getting keys for pattern: {}", pattern, e);
                return List.of();
            }
        });
    }

    public List<String> getKeysStartingWith(String prefix) {
        return RetryUtil.retry(() -> {
            try {
                byte[] prefixBytes = toBytes(prefix);
                List<String> keys = new ArrayList<>();
                try (RocksIterator it = db.newIterator()) {
                    for (it.seek(prefixBytes); it.isValid(); it.next()) {
                        byte[] key = it.key();
                        if (!startsWith(key, prefixBytes)) break;
                        keys.add(fromBytes(key));
                    }
                }
                logger.debug("Found {} keys starting with: {}", keys.size(), prefix);
                return keys;
            } catch (Exception e) {
                logger.error("Error getting keys starting with: {}", prefix, e);
                return new ArrayList<>();
            }
        });
    }

    // -----------------------------------------------------------------
    // Utility methods
    // -----------------------------------------------------------------
    private static byte[] toBytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    private static String fromBytes(byte[] b) { return new String(b, StandardCharsets.UTF_8); }
    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) return false;
        }
        return true;
    }

    /** Close all resources */
    public void close() {
        writer.shutdown();
        try {
            cfCard.close();
            cfMerchant.close();
            cfCustom.close();
            cfDefault.close();
            db.close();
        } catch (Exception e) {
            logger.error("Error closing RocksDB resources", e);
        }
    }

    // -----------------------------------------------------------------
    // Async writer – now receives the RocksDB instance and supports CFs
    // -----------------------------------------------------------------
    private static class AsyncRocksDbWriter {
        private static final Logger logger = LoggerFactory.getLogger(AsyncRocksDbWriter.class);
        private static volatile AsyncRocksDbWriter instance;

        private final RocksDB db;
        private final int shardCount;
        private final List<BlockingQueue<WriteRequest>> queues;
        private final List<Thread> workers;
        private final ThreadLocal<Fury> threadLocalFury = ThreadLocal.withInitial(() ->
                Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build()
        );
        @SuppressWarnings("resource")
        private final WriteOptions writeOptions = new WriteOptions().setSync(false);
        private volatile boolean running = true;
        private final long submitTimeoutMs;

        static { RocksDB.loadLibrary(); }

        private AsyncRocksDbWriter(RocksDB db, int queueSize, long submitTimeoutMs, int shardCount) {
            this.db = db;
            this.submitTimeoutMs = submitTimeoutMs;
            this.shardCount = shardCount;
            this.queues = new ArrayList<>(shardCount);
            this.workers = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; i++) {
                BlockingQueue<WriteRequest> q = new ArrayBlockingQueue<>(queueSize);
                queues.add(q);
                int shardId = i;
                Thread t = Thread.ofVirtual().name("rocksdb-writer-" + i)
                        .start(() -> runWorker(shardId, q));
                workers.add(t);
            }
            logger.info("AsyncRocksDbWriter started with {} shards, queue size {} per shard", shardCount, queueSize);
        }

        public static AsyncRocksDbWriter getInstance(RocksDB db, int queueSize) {
            if (instance == null) {
                synchronized (AsyncRocksDbWriter.class) {
                    if (instance == null) {
                        instance = new AsyncRocksDbWriter(db, queueSize, AppConfig.rocksDBSubmitTimeoutMs, AppConfig.rocksDBShardCount);
                    }
                }
            }
            return instance;
        }

        private int getShardIndex(byte[] key) {
            return Math.abs(Arrays.hashCode(key)) % shardCount;
        }

        /** Submit a put operation for a specific column family */
        public boolean submitPutObjectWithCF(ColumnFamilyHandle cf, byte[] key, Object valueObj) {
            int shard = getShardIndex(key);
            BlockingQueue<WriteRequest> queue = queues.get(shard);
            WriteRequest req = WriteRequest.putObjectWithCF(cf, key, valueObj);
            if (queue.offer(req)) return true;
            try {
                if (queue.offer(req, submitTimeoutMs, TimeUnit.MILLISECONDS)) return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // fallback direct write
            try (WriteBatch batch = new WriteBatch()) {
                byte[] serialized = threadLocalFury.get().serialize(valueObj);
                batch.put(cf, key, serialized);
                db.write(writeOptions, batch);
                return true;
            } catch (Exception e) {
                logger.error("Fallback direct write failed for key {}", new String(key, StandardCharsets.UTF_8), e);
                return false;
            }
        }

        /** Delete operation (column‑family aware) */
        public boolean submitDelete(ColumnFamilyHandle cf, byte[] key) {
            int shard = getShardIndex(key);
            BlockingQueue<WriteRequest> queue = queues.get(shard);
            WriteRequest req = WriteRequest.del(cf, key);
            if (queue.offer(req)) return true;
            try {
                if (queue.offer(req, submitTimeoutMs, TimeUnit.MILLISECONDS)) return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // fallback direct delete
            try {
                db.delete(cf, writeOptions, key);
                return true;
            } catch (Exception e) {
                logger.error("Fallback direct delete failed for key {}", new String(key, StandardCharsets.UTF_8), e);
                return false;
            }
        }

        private void runWorker(int shardId, BlockingQueue<WriteRequest> queue) {
            logger.info("RocksDB writer shard {} started", shardId);
            while (running) {
                try {
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
                    // shutdown signal
                } catch (Exception e) {
                    logger.error("Error in RocksDB writer thread (shard {})", shardId, e);
                }
            }
        }

        private void apply(WriteBatch batch, WriteRequest req) throws RocksDBException {
            switch (req.type) {
                case PUT_BINARY -> batch.put(req.key, req.value);
                case PUT_OBJECT -> {
                    byte[] ser = threadLocalFury.get().serialize(req.objectValue);
                    batch.put(req.key, ser);
                }
                case PUT_OBJECT_WITH_CF -> {
                    byte[] ser = threadLocalFury.get().serialize(req.objectValue);
                    batch.put(req.cfHandle, req.key, ser);
                }
                case DEL -> batch.delete(req.cfHandle, req.key);
            }
        }

        public void shutdown() {
            running = false;
            workers.forEach(Thread::interrupt);
        }

        /** Internal request representation */
        private static class WriteRequest {
            enum Type {PUT_BINARY, PUT_OBJECT, PUT_OBJECT_WITH_CF, DEL}
            final Type type;
            final ColumnFamilyHandle cfHandle; // may be null for default CF
            final byte[] key;
            final byte[] value; // for binary
            final Object objectValue; // for object writes

            private WriteRequest(Type type, ColumnFamilyHandle cfHandle, byte[] key, byte[] value, Object objectValue) {
                this.type = type;
                this.cfHandle = cfHandle;
                this.key = key;
                this.value = value;
                this.objectValue = objectValue;
            }

            static WriteRequest putObjectWithCF(ColumnFamilyHandle cf, byte[] key, Object obj) {
                return new WriteRequest(Type.PUT_OBJECT_WITH_CF, cf, key, null, obj);
            }

            static WriteRequest del(ColumnFamilyHandle cf, byte[] key) {
                return new WriteRequest(Type.DEL, cf, key, null, null);
            }
        }
    }
}
