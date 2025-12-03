package ma.s2m.fraudmanager.service;

import ma.medtech.droolbuilder.rules.Subject;
import ma.s2m.fraudmanager.config.AppConfig;
import ma.s2m.fraudmanager.model.Measurment;
import ma.s2m.fraudmanager.model.WrapperMeasurment;
import ma.s2m.fraudmanager.util.RetryUtil;
import ma.s2m.functions.Function;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * RocksDB service with asynchronous writes.
 * - Writes are delegated to a background writer thread.
 * - Objects are serialized inside the writer using {@link KryoSerializationService}.
 * - Uses a column family per subject (CARD, MERCHANT, CUSTOM) and a
 *   prefix‑rich key format: <subject>[:<custom>]:<key>:<windowSize>.
 */
public class RocksDBService {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBService.class);

    // Jackson mapper for legacy JSON maps
    private final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<Map<Long, Measurment>> MEAS_MAP_TYPE =
            new TypeReference<Map<Long, Measurment>>() {};

    // Multi-shard RocksDB handles
    private final Map<Integer, RocksDB> shardedDbs;
    private final Map<Integer, ColumnFamilyHandle> cfDefaultByShard;
    private final Map<Integer, ColumnFamilyHandle> cfCardByShard;
    private final Map<Integer, ColumnFamilyHandle> cfMerchantByShard;
    private final Map<Integer, ColumnFamilyHandle> cfCustomByShard;
    
    // Shard configuration
    private final int totalDiskShards;
    private final List<Integer> assignedShards;

    // Async writers per shard
    private final Map<Integer, AsyncRocksDbWriter> shardWriters;

    // ThreadLocal batch accumulator for reducing serialization overhead
    private final ThreadLocal<Map<String, Measurment>> batchAccumulator = 
        ThreadLocal.withInitial(HashMap::new);
    
    // ThreadLocal batch accumulator for WrapperMeasurment
    private final ThreadLocal<Map<String, WrapperMeasurment>> wrapperBatchAccumulator = 
        ThreadLocal.withInitial(HashMap::new);

    public RocksDBService(String dbPath, int queueSize) {
        // -----------------------------------------------------------------
        // Initialize shard configuration
        // -----------------------------------------------------------------
        this.totalDiskShards = AppConfig.rocksDBDiskShardCount;
        this.assignedShards = new ArrayList<>(AppConfig.rocksDBShards);
        if (this.assignedShards.isEmpty()) {
            // Fallback: if no shards configured, assign shard 0
            logger.warn("No shards assigned to this node, defaulting to shard 0");
            this.assignedShards.add(0);
        }
        
        this.shardedDbs = new HashMap<>();
        this.cfDefaultByShard = new HashMap<>();
        this.cfCardByShard = new HashMap<>();
        this.cfMerchantByShard = new HashMap<>();
        this.cfCustomByShard = new HashMap<>();
        this.shardWriters = new HashMap<>();
        
        logger.info("Initializing RocksDB with {} total disk shards, assigned shards: {}", 
                    totalDiskShards, assignedShards);

        // -----------------------------------------------------------------
        // Open RocksDB instance for each assigned shard
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
            
            @SuppressWarnings("resource")
            DBOptions dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    .setIncreaseParallelism(Runtime.getRuntime().availableProcessors());
            
            for (int shardId : assignedShards) {
                String shardPath = dbPath + File.separator + "shard-" + shardId;
                logger.info("Node: {}: Opening RocksDB shard {} at {}", AppConfig.rocksdbNodeName, shardId, shardPath);
                
                File shardDir = new File(shardPath);
                if (!shardDir.exists() && !shardDir.mkdirs()) {
                    throw new RuntimeException("Failed to create directory for RocksDB shard: " + shardPath);
                }
                
                List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
                RocksDB db = RocksDB.open(dbOptions, shardPath, cfDescriptors, cfHandles);
                
                shardedDbs.put(shardId, db);
                cfDefaultByShard.put(shardId, cfHandles.get(0));
                cfCardByShard.put(shardId, cfHandles.get(1));
                cfMerchantByShard.put(shardId, cfHandles.get(2));
                cfCustomByShard.put(shardId, cfHandles.get(3));
                
                // Create async writer for this shard
                AsyncRocksDbWriter writer = AsyncRocksDbWriter.createInstance(db, queueSize, shardId);
                shardWriters.put(shardId, writer);
                
                logger.info("Successfully opened RocksDB shard {}", shardId);
            }
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to open RocksDB shards", e);
        }
    }

    // -----------------------------------------------------------------
    // SHARD ROUTING
    // -----------------------------------------------------------------
        
    /** Helper to pick the correct column family based on the key prefix for a specific shard */
    private ColumnFamilyHandle getCFHandleForKey(int shardId, String key) {
        // Expected key format: subject[:custom]:key:windowSize
        int firstSep = key.indexOf(FraudProcessor.KEY_SEPARATOR);
        if (firstSep == -1) {
            return cfDefaultByShard.get(shardId);
        }
        String subjectPart = key.substring(0, firstSep);
        switch (subjectPart) {
            case Subject.CARD:
                return cfCardByShard.get(shardId);
            case Subject.MERCHANT:
                return cfMerchantByShard.get(shardId);
            default:
                if (subjectPart.startsWith(Subject.CUSTOM)) {
                    return cfCustomByShard.get(shardId);
                }
                return cfDefaultByShard.get(shardId);
        }
    }

    // -----------------------------------------------------------------
    // READ APIs
    // -----------------------------------------------------------------
    public Map<Long, Measurment> getMeasurments(String key) {
        return RetryUtil.retry(() -> {
            try {
                int shardId = Function.calculateShardId(key, totalDiskShards);
                RocksDB db = shardedDbs.get(shardId);
                if (db == null) {
                    logger.warn("Shard {} is not assigned to this node, key: {}", shardId, key);
                    return new HashMap<>();
                }
                ColumnFamilyHandle cf = getCFHandleForKey(shardId, key);
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
            int shardId = Function.calculateShardId(key, totalDiskShards);
            RocksDB db = shardedDbs.get(shardId);
            if (db == null) {
                logger.warn("Shard {} is not assigned to this node, key: {}", shardId, key);
                return null;
            }
            ColumnFamilyHandle cf = getCFHandleForKey(shardId, key);
            byte[] v = db.get(cf, toBytes(key));
            if (v != null) {
                return KryoSerializationService.deserialize(v, Measurment.class);
            }
        } catch (Exception e) {
            logger.error("Error retrieving Measurment from RocksDB for key {}: {}", key, e.getMessage(), e);
        }
        return null;
    }

    public WrapperMeasurment getWrapperMeasurmentByKey(String key) {
        try {
            int shardId = Function.calculateShardId(key, totalDiskShards);
            RocksDB db = shardedDbs.get(shardId);
            if (db == null) {
                logger.warn("Shard {} is not assigned to this node, key: {}", shardId, key);
                return null;
            }
            ColumnFamilyHandle cf = getCFHandleForKey(shardId, key);
            byte[] v = db.get(cf, toBytes(key));
            if (v != null) {
                return KryoSerializationService.deserialize(v, WrapperMeasurment.class);
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
            int shardId = Function.calculateShardId(key, totalDiskShards);
            AsyncRocksDbWriter writer = shardWriters.get(shardId);
            if (writer == null) {
                logger.warn("Shard {} is not assigned to this node, key: {}", shardId, key);
                return;
            }
            ColumnFamilyHandle cf = getCFHandleForKey(shardId, key);
            boolean ok = writer.submitPutObjectWithCF(cf, toBytes(key), measurments);
            if (!ok) {
                logger.warn("Failed to persist measurments for key {} (queue full and fallback failed)", key);
            } else {
                logger.debug("Persisted measurments for key {}", key);
            }
        });
    }

    public void setMeasurmentByKey(String key, Measurment measurment) {
        // Accumulate in batch instead of saving immediately
        batchAccumulator.get().put(key, measurment);
        logger.debug("Accumulated measurment for key {} in batch", key);
    }

    public void setWrapperMeasurmentByKey(String key, WrapperMeasurment wrapperMeasurment) {
        // Accumulate in batch instead of saving immediately
        wrapperBatchAccumulator.get().put(key, wrapperMeasurment);
        logger.debug("Accumulated wrapper measurment for key {} in batch", key);
    }

    /**
     * Extracts the base key for sharding purposes (removes window size suffix).
     * Ensures consistency with the router which routes based on Subject:Key.
     */
    private String getShardKey(String key) {
        int lastSlash = key.lastIndexOf('/');
        if (lastSlash != -1) {
            return key.substring(0, lastSlash);
        }
        return key;
    }

    /**
     * Flushes all accumulated writes in the current thread's batch.
     * This dramatically reduces serialization overhead by batching multiple writes.
     * Call this at the end of processing a NATS message.
     */
    public void flushBatch() {
        Map<String, Measurment> batch = batchAccumulator.get();
        Map<String, WrapperMeasurment> wrapperBatch = wrapperBatchAccumulator.get();
        
        if (batch.isEmpty() && wrapperBatch.isEmpty()) {
            return;
        }

        long start = System.currentTimeMillis();
        int submitted = 0;
        int failed = 0;

        // ===== Flush Measurment batch =====
        if (!batch.isEmpty()) {
            // Group by shard for efficiency
            Map<Integer, List<Map.Entry<String, Measurment>>> byShard = new HashMap<>();
            
            for (Map.Entry<String, Measurment> entry : batch.entrySet()) {
                // Use base key (without window size) for shard calculation
                String shardKey = getShardKey(entry.getKey());
                int shardId = Function.calculateShardId(shardKey, totalDiskShards);
                byShard.computeIfAbsent(shardId, k -> new ArrayList<>()).add(entry);
            }

            // Submit batch for each shard
            for (Map.Entry<Integer, List<Map.Entry<String, Measurment>>> shardEntry : byShard.entrySet()) {
                int shardId = shardEntry.getKey();
                AsyncRocksDbWriter writer = shardWriters.get(shardId);
                
                if (writer == null) {
                    logger.warn("Shard {} not assigned, skipping {} measurments (key sample: {})", 
                               shardId, shardEntry.getValue().size(), shardEntry.getValue().get(0).getKey());
                    failed += shardEntry.getValue().size();
                    continue;
                }

                // Submit each measurment in the batch
                for (Map.Entry<String, Measurment> entry : shardEntry.getValue()) {
                    String key = entry.getKey();
                    ColumnFamilyHandle cf = getCFHandleForKey(shardId, key);
                    
                    RetryUtil.retry(() -> {
                        boolean ok = writer.submitPutObjectWithCF(cf, toBytes(key), entry.getValue());
                        if (!ok) {
                            logger.warn("Failed to persist measurment for key {} in batch", key);
                        }
                    });
                    submitted++;
                }
            }

            // Clear the batch for this thread
            batch.clear();
        }

        // ===== Flush WrapperMeasurment batch =====
        if (!wrapperBatch.isEmpty()) {
            // Group by shard for efficiency
            Map<Integer, List<Map.Entry<String, WrapperMeasurment>>> byShard = new HashMap<>();
            
            for (Map.Entry<String, WrapperMeasurment> entry : wrapperBatch.entrySet()) {
                // Use base key (without window size) for shard calculation
                String tmpKey = entry.getKey();
                tmpKey = tmpKey.replace(FraudProcessor.FIXED_WINDOW_PREFIX, "");
                String shardKey = getShardKey(tmpKey);
                int shardId = Function.calculateShardId(shardKey, totalDiskShards);
                byShard.computeIfAbsent(shardId, k -> new ArrayList<>()).add(entry);
            }

            // Submit batch for each shard
            for (Map.Entry<Integer, List<Map.Entry<String, WrapperMeasurment>>> shardEntry : byShard.entrySet()) {
                int shardId = shardEntry.getKey();
                AsyncRocksDbWriter writer = shardWriters.get(shardId);
                
                if (writer == null) {
                    logger.warn("Shard {} not assigned, skipping {} wrapper measurments", 
                               shardId, shardEntry.getValue().size());
                    failed += shardEntry.getValue().size();
                    continue;
                }

                // Submit each wrapper measurment in the batch
                for (Map.Entry<String, WrapperMeasurment> entry : shardEntry.getValue()) {
                    String key = entry.getKey();
                    ColumnFamilyHandle cf = getCFHandleForKey(shardId, key);
                    
                    RetryUtil.retry(() -> {
                        boolean ok = writer.submitPutObjectWithCF(cf, toBytes(key), entry.getValue());
                        if (!ok) {
                            logger.warn("Failed to persist wrapper measurment for key {} in batch", key);
                        }
                    });
                    submitted++;
                }
            }

            // Clear the wrapper batch for this thread
            wrapperBatch.clear();
        }

        long end = System.currentTimeMillis();
        logger.debug("Time {} ms : flushBatch {} keys ({} submitted, {} failed)", 
                    (end - start), submitted + failed, submitted, failed);
    }

    // -----------------------------------------------------------------
    // DELETE API (column‑family aware)
    // -----------------------------------------------------------------
    public void deleteKey(String key) {
        RetryUtil.retry(() -> {
            int shardId = Function.calculateShardId(key, totalDiskShards);
            AsyncRocksDbWriter writer = shardWriters.get(shardId);
            if (writer == null) {
                logger.warn("Shard {} is not assigned to this node, key: {}", shardId, key);
                return;
            }
            ColumnFamilyHandle cf = getCFHandleForKey(shardId, key);
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
                    // Search across all assigned shards
                    for (int shardId : assignedShards) {
                        RocksDB db = shardedDbs.get(shardId);
                        if (db == null) continue;
                        try (RocksIterator it = db.newIterator()) {
                            for (it.seek(prefixBytes); it.isValid(); it.next()) {
                                byte[] key = it.key();
                                if (!startsWith(key, prefixBytes)) break;
                                keys.add(fromBytes(key));
                            }
                        }
                    }
                    logger.debug("Found {} keys matching prefix pattern: {}", keys.size(), pattern);
                    return keys;
                } else {
                    // Exact match - calculate shard
                    int shardId = Function.calculateShardId(pattern, totalDiskShards);
                    RocksDB db = shardedDbs.get(shardId);
                    if (db == null) return List.of();
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
                // Search across all assigned shards
                for (int shardId : assignedShards) {
                    RocksDB db = shardedDbs.get(shardId);
                    if (db == null) continue;
                    try (RocksIterator it = db.newIterator()) {
                        for (it.seek(prefixBytes); it.isValid(); it.next()) {
                            byte[] key = it.key();
                            if (!startsWith(key, prefixBytes)) break;
                            keys.add(fromBytes(key));
                        }
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
    // Simple lock stubs (kept for compatibility)
    // -----------------------------------------------------------------
    public boolean acquireLock(String lockKey, String lockValue) { return true; }
    public boolean releaseLock(String lockKey, String lockValue) { return true; }
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
        // Shutdown all shard writers
        for (AsyncRocksDbWriter writer : shardWriters.values()) {
            writer.shutdown();
        }
        
        // Close all column families and databases for each shard
        try {
            for (int shardId : assignedShards) {
                ColumnFamilyHandle cfCard = cfCardByShard.get(shardId);
                ColumnFamilyHandle cfMerchant = cfMerchantByShard.get(shardId);
                ColumnFamilyHandle cfCustom = cfCustomByShard.get(shardId);
                ColumnFamilyHandle cfDefault = cfDefaultByShard.get(shardId);
                RocksDB db = shardedDbs.get(shardId);
                
                if (cfCard != null) cfCard.close();
                if (cfMerchant != null) cfMerchant.close();
                if (cfCustom != null) cfCustom.close();
                if (cfDefault != null) cfDefault.close();
                if (db != null) db.close();
                
                logger.info("Closed RocksDB shard {}", shardId);
            }
        } catch (Exception e) {
            logger.error("Error closing RocksDB resources", e);
        }
    }

    // -----------------------------------------------------------------
    // Async writer – per shard instance
    // ----------------------------------------------------------------- 
    private static class AsyncRocksDbWriter {
        private static final Logger logger = LoggerFactory.getLogger(AsyncRocksDbWriter.class);
        private final RocksDB db;
        private final int shardCount;
        private final List<BlockingQueue<WriteRequest>> queues;
        private final List<Thread> workers;
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

        /** Create a  new instance for a specific shard - not using singleton pattern */
        public static AsyncRocksDbWriter createInstance(RocksDB db, int queueSize, int shardId) {
            return new AsyncRocksDbWriter(db, queueSize, AppConfig.rocksDBSubmitTimeoutMs, AppConfig.rocksDBMemoryShardCount);
        }

        private int getMemoryShardIndex(byte[] key) {
            return Math.abs(Arrays.hashCode(key)) % shardCount;
        }

        /** Submit a put operation for a specific column family */
        public boolean submitPutObjectWithCF(ColumnFamilyHandle cf, byte[] key, Object valueObj) {
            int shard = getMemoryShardIndex(key);
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
                byte[] serialized = KryoSerializationService.serialize(valueObj);
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
            int shard = getMemoryShardIndex(key);
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
                    byte[] ser = KryoSerializationService.serialize(req.objectValue);
                    batch.put(req.key, ser);
                }
                case PUT_OBJECT_WITH_CF -> {
                    byte[] ser = KryoSerializationService.serialize(req.objectValue);
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
