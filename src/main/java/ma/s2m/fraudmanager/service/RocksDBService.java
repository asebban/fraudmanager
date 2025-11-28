package ma.s2m.fraudmanager.service;

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
 * - Objects are serialized inside the writer using {@link SerializationManager}.
 */
public class RocksDBService {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBService.class);

    // Jackson mapper for reading JSON maps (legacy compatibility).
    private final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<Map<Long, Measurment>> MEAS_MAP_TYPE = new TypeReference<Map<Long, Measurment>>() {};

    private final RocksDB db;
    private final AsyncRocksDbWriter writer;
    
    // Thread-safe Fury instance for synchronous reads
    private final ThreadSafeFury readFury;

    public RocksDBService(String dbPath, int queueSize) {
        this.writer = AsyncRocksDbWriter.getInstance(dbPath, queueSize);
        this.db = writer.db();
        // Initialize Fury for reading (thread-safe)
        this.readFury = Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).buildThreadSafeFury();
    }

    /**
     * Synchronous read of a map of measurements.
     */
    public Map<Long, Measurment> getMeasurments(String key) {
        return RetryUtil.retry(() -> {
            try {
                byte[] value = db.get(toBytes(key));
                if (value == null) {
                    logger.debug("Key {} not found, returning empty map", key);
                    return new HashMap<>();
                }
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
     * Asynchronous write of a map of measurements.
     */
    public void setMeasurments(String key, Map<Long, Measurment> measurments) {
        RetryUtil.retry(() -> {
            byte[] k = toBytes(key);
            boolean ok = writer.submitPutObject(k, measurments);
            if (!ok) {
                logger.warn("Failed to persist measurments for key {} (queue full and fallback failed)", key);
            } else {
                logger.debug("Persisted measurments for key {} (async or direct fallback)", key);
            }
        });
    }

    /**
     * Asynchronous write of a single measurement.
     */
    public void setMeasurmentByKey(String key, Measurment measurment) {
        long start = System.currentTimeMillis();
        RetryUtil.retry(() -> {
            byte[] k = toBytes(key);
            boolean ok = writer.submitPutObject(k, measurment);
            if (!ok) {
                logger.warn("Failed to persist measurment for key {} (queue full and fallback failed)", key);
            } else {
                logger.debug("Persisted measurment for key {}", key);
            }
        });
        long end = System.currentTimeMillis();
        logger.debug("Time {} ms : setMeasurmentByKey for key {}", (end - start), key);
    }

    /**
     * Asynchronous write of a wrapper measurement.
     */
    public void setWrapperMeasurmentByKey(String key, WrapperMeasurment wrapperMeasurment) {
        long start = System.currentTimeMillis();
        RetryUtil.retry(() -> {
            byte[] k = toBytes(key);
            boolean ok = writer.submitPutObject(k, wrapperMeasurment);
            if (!ok) {
                logger.warn("Failed to persist WrapperMeasurment for key {} (queue full and fallback failed)", key);
            } else {
                logger.debug("Persisted WrapperMeasurment for key {}", key);
            }
        });
        long end = System.currentTimeMillis();
        logger.debug("Time {} ms : setWrapperMeasurmentByKey for key {}", (end - start), key);
    }

    /**
     * Simple lock implementation – currently a stub that always succeeds.
     * Replace with a proper distributed lock if needed.
     */
    public boolean acquireLock(String lockKey, String lockValue) {
        return true;
    }

    public boolean releaseLock(String lockKey, String lockValue) {
        return true;
    }

    /**
     * Asynchronous delete of a key.
     */
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
                        if (!startsWith(key, prefixBytes)) {
                            break;
                        }
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

    /**
     * Synchronous get of a Measurment by key.
     */
    public Measurment getMeasurmentByKey(String key) {
        try {
            byte[] v = db.get(toBytes(key));
            if (v != null) {
                return (Measurment) readFury.deserialize(v);
            }
        } catch (Exception e) {
            logger.error("Error retrieving Measurment from RocksDB for key {}: {}", key, e.getMessage(), e);
        }
        return null;
    }

    /**
     * Synchronous get of a WrapperMeasurment by key.
     */
    public WrapperMeasurment getWrapperMeasurmentByKey(String key) {
        try {
            byte[] v = db.get(toBytes(key));
            if (v != null) {
                return (WrapperMeasurment) readFury.deserialize(v);
            }
        } catch (Exception e) {
            logger.error("Error retrieving WrapperMeasurment from RocksDB for key {}: {}", key, e.getMessage(), e);
        }
        return null;
    }

    /**
     * Closes the RocksDB instance.
     */
    public void close() {
        writer.shutdown();
    }

    /**
     * Converts a string to a byte array.
     */
    private static byte[] toBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Converts a byte array to a string.
     */
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

    /**
     * ============================================================
     * =============== WRITER ASYNC DÉPORTÉ (SHARDED) ============
     * ============================================================
     */
    private static class AsyncRocksDbWriter {
        private static final Logger logger = LoggerFactory.getLogger(AsyncRocksDbWriter.class);
        private static volatile AsyncRocksDbWriter instance;

        private final RocksDB db;
        // Sharded queues and workers
        private final int shardCount;
        private final List<BlockingQueue<WriteRequest>> queues;
        private final List<Thread> workers;
        
        // ThreadLocal Fury for writers (zero contention)
        private final ThreadLocal<Fury> threadLocalFury = ThreadLocal.withInitial(() -> 
            Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build()
        );
        
        private final WriteOptions writeOptions;
        private volatile boolean running = true;
        // Timeout (ms) to wait when the queue is full before falling back to a direct write.
        private final long submitTimeoutMs;

        static {
            RocksDB.loadLibrary();
        }

        @SuppressWarnings("resource")
        private AsyncRocksDbWriter(String dbPath, int queueSize, long submitTimeoutMs, int shardCount) {
            this.submitTimeoutMs = submitTimeoutMs;
            this.shardCount = shardCount;
            try {
                Options options = new Options()
                        .setCreateIfMissing(true)
                        .setCompressionType(CompressionType.LZ4_COMPRESSION);
                // Enable parallel flush/compaction if needed, but here we just open the DB
                this.db = RocksDB.open(options, dbPath);
            } catch (RocksDBException e) {
                throw new RuntimeException("Cannot open RocksDB at " + dbPath, e);
            }

            this.writeOptions = new WriteOptions().setSync(false);
            this.queues = new ArrayList<>(shardCount);
            this.workers = new ArrayList<>(shardCount);

            for (int i = 0; i < shardCount; i++) {
                BlockingQueue<WriteRequest> q = new ArrayBlockingQueue<>(queueSize);
                queues.add(q);
                int shardId = i;
                Thread t = Thread.ofVirtual().name("rocksdb-writer-" + i).start(() -> runWorker(shardId, q));
                workers.add(t);
            }
            
            // No need to call workers.forEach(Thread::start); as virtual threads are started immediately.
            logger.info("AsyncRocksDbWriter started with {} shards, queue size {} per shard", shardCount, queueSize);
        }
        
        static {
            RocksDB.loadLibrary();
        }

        public static AsyncRocksDbWriter getInstance(String dbPath, int queueSize) {
            if (instance == null) {
                synchronized (AsyncRocksDbWriter.class) {
                    if (instance == null) {
                        // Default: 100ms timeout, 4 shards
                        instance = new AsyncRocksDbWriter(dbPath, queueSize, 100L, 4);
                    }
                }
            }
            return instance;
        }

        public RocksDB db() {
            return db;
        }

        private int getShardIndex(byte[] key) {
            if (key == null) return 0;
            return Math.abs(Arrays.hashCode(key)) % shardCount;
        }

        /**
         * Enqueue an object write. If the queue is full we block for {@code submitTimeoutMs}
         * and, if still full, perform a direct write to guarantee durability.
         */
        public boolean submitPutObject(byte[] key, Object valueObj) {
            int shard = getShardIndex(key);
            BlockingQueue<WriteRequest> queue = queues.get(shard);

            // Fast non‑blocking attempt.
            if (queue.offer(WriteRequest.putObject(key, valueObj))) {
                return true;
            }
            
            logger.debug("[key = {}] Shard {} queue full (size={}), retrying with timeout...", 
                    new String(key, StandardCharsets.UTF_8), shard, queue.size());

            // Queue full – block for a short period.
            try {
                if (queue.offer(WriteRequest.putObject(key, valueObj), submitTimeoutMs, TimeUnit.MILLISECONDS)) {
                    return true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Still full – fallback to direct write.
            logger.warn("Async queue (shard {}) still full after timeout, performing direct write for key {}", shard, new String(key, StandardCharsets.UTF_8));
            
            try (WriteBatch batch = new WriteBatch()) {
                byte[] serialized = threadLocalFury.get().serialize(valueObj);
                batch.put(key, serialized);
                db.write(writeOptions, batch);
                return true;
            } catch (Exception e) {
                logger.error("Fallback direct write failed for key {}", new String(key, StandardCharsets.UTF_8), e);
                return false;
            }
        }

        public boolean submitDelete(byte[] key) {
            int shard = getShardIndex(key);
            return queues.get(shard).offer(WriteRequest.del(key));
        }

        private void runWorker(int shardId, BlockingQueue<WriteRequest> queue) {
            logger.info("RocksDB writer shard {} started", shardId);
            while (running) {
                try {
                    WriteRequest first = queue.take();
                    try (WriteBatch batch = new WriteBatch()) {
                        apply(batch, first);
                        int drained = 0;
                        // Batch up to 200 items from the queue
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
                    byte[] serialized = threadLocalFury.get().serialize(req.objectValue);
                    batch.put(req.key, serialized);
                }
                case DEL -> batch.delete(req.key);
            }
        }

        public void shutdown() {
            running = false;
            workers.forEach(Thread::interrupt);
            db.close();
        }

        private static class WriteRequest {
            enum Type {PUT_BINARY, PUT_OBJECT, DEL}
            final Type type;
            final byte[] key;
            final byte[] value; // for PUT_BINARY
            final Object objectValue; // for PUT_OBJECT

            private WriteRequest(Type type, byte[] key, byte[] value, Object objectValue) {
                this.type = type;
                this.key = key;
                this.value = value;
                this.objectValue = objectValue;
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
