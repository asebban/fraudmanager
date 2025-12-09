package ma.s2m.fraudmanager.service;

import ma.medtech.droolbuilder.rules.Subject;
import ma.s2m.fraudmanager.config.AppConfig;
import ma.s2m.fraudmanager.model.Measurment;
import ma.s2m.fraudmanager.model.RecordHashMap;
import ma.s2m.fraudmanager.model.WrapperMeasurment;
import ma.s2m.functions.Function;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class EclipseStoreService implements IStoreService {
    private static final Logger logger = LoggerFactory.getLogger(EclipseStoreService.class);

    // Shard configuration
    private final int totalDiskShards;
    private final List<Integer> assignedShards;

    // EclipseStore instances per shard
    private final Map<Integer, EmbeddedStorageManager> shardManagers = new HashMap<>();
    private final Map<Integer, StorageRoot> shardRoots = new HashMap<>();

    public EclipseStoreService(String dbPath) {
        this.totalDiskShards = AppConfig.storageDiskShardCount;
        this.assignedShards = new ArrayList<>(AppConfig.storageShards);

        if (this.assignedShards.isEmpty()) {
            logger.warn("No shards assigned to this node, defaulting to shard 0");
            this.assignedShards.add(0);
        }

        logger.info("Initializing EclipseStore with {} total disk shards, assigned shards: {}",
                totalDiskShards, assignedShards);

        for (int shardId : assignedShards) {
            String shardPath = dbPath + File.separator + "shard-" + shardId;
            logger.info("Node: {}: Opening EclipseStore shard {} at {}", AppConfig.nodeName, shardId, shardPath);

            StorageRoot root = new StorageRoot();
            EmbeddedStorageManager storageManager = EmbeddedStorage.start(
                    root,
                    Paths.get(shardPath)
            );

            shardManagers.put(shardId, storageManager);
            shardRoots.put(shardId, root);
            logger.info("Successfully opened EclipseStore shard {}", shardId);
        }
    }

    // -----------------------------------------------------------------
    // Data Root Structure
    // -----------------------------------------------------------------
    public static class StorageRoot {
        // Maps corresponding to RocksDB Column Families
        private final Map<String, Object> defaultMap = new ConcurrentHashMap<>();
        private final Map<String, Object> cardMap = new ConcurrentHashMap<>();
        private final Map<String, Object> merchantMap = new ConcurrentHashMap<>();
        private final Map<String, Object> customMap = new ConcurrentHashMap<>();

        public Map<String, Object> getMapForKey(String key) {
            int firstSep = key.indexOf(FraudProcessor.KEY_SEPARATOR);
            if (firstSep == -1) {
                return defaultMap;
            }
            String subjectPart = key.substring(0, firstSep);
            switch (subjectPart) {
                case Subject.CARD:
                    return cardMap;
                case Subject.MERCHANT:
                    return merchantMap;
                default:
                    if (subjectPart.startsWith(Subject.CUSTOM)) {
                        return customMap;
                    }
                    return defaultMap;
            }
        }
        
        public List<Map<String, Object>> getAllMaps() {
            return Arrays.asList(defaultMap, cardMap, merchantMap, customMap);
        }
    }

    private StorageRoot getRootForShard(int shardId) {
        return shardRoots.get(shardId);
    }

    private EmbeddedStorageManager getManagerForShard(int shardId) {
        return shardManagers.get(shardId);
    }

    // -----------------------------------------------------------------
    // READ APIs
    // -----------------------------------------------------------------
    @Override
    @SuppressWarnings("unchecked")
    public Map<Long, Measurment> getMeasurments(String key) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        if (root == null) {
            logger.warn("Shard {} is not assigned to this node, key: {}", shardId, key);
            return new HashMap<>();
        }
        Map<String, Object> map = root.getMapForKey(key);
        return (Map<Long, Measurment>) map.getOrDefault(key, new HashMap<>());
    }

    @Override
    public RecordHashMap getRecordHashMapByKey(String key) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        if (root == null) return null;
        Map<String, Object> map = root.getMapForKey(key);
        return (RecordHashMap) map.get(key);
    }

    @Override
    public Measurment getMeasurmentByKey(String key) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        if (root == null) return null;
        Map<String, Object> map = root.getMapForKey(key);
        return (Measurment) map.get(key);
    }

    @Override
    public WrapperMeasurment getWrapperMeasurmentByKey(String key) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        if (root == null) return null;
        Map<String, Object> map = root.getMapForKey(key);
        return (WrapperMeasurment) map.get(key);
    }

    // -----------------------------------------------------------------
    // WRITE APIs
    // -----------------------------------------------------------------
    @Override
    public void setMeasurments(String key, Map<Long, Measurment> measurments) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        
        if (root != null && manager != null) {
            Map<String, Object> map = root.getMapForKey(key);
            map.put(key, measurments);
            manager.store(map);
        }
    }

    @Override
    public void setRecordHashMapByKey(String key, RecordHashMap recordHashMap) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        
        if (root != null && manager != null) {
            Map<String, Object> map = root.getMapForKey(key);
            map.put(key, recordHashMap);
            manager.store(map);
        }
    }

    @Override
    public void setMeasurmentByKey(String key, Measurment measurment) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        
        if (root != null && manager != null) {
            Map<String, Object> map = root.getMapForKey(key);
            map.put(key, measurment);
            manager.store(map);
        }
    }

    @Override
    public void setWrapperMeasurmentByKey(String key, WrapperMeasurment wrapperMeasurment) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        
        if (root != null && manager != null) {
            Map<String, Object> map = root.getMapForKey(key);
            map.put(key, wrapperMeasurment);
            manager.store(map);
        }
    }

    @Override
    public void flushBatch() {
        // EclipseStore handles persistence automatically or via explicit store calls.
        // Since we call store() in setters, this might be a no-op or we could optimize by batching store calls if we change the setters.
        // For now, keeping it empty as setters persist immediately.
        // To optimize, we could implement a similar batching mechanism as RocksDBService.
    }

    @Override
    public void deleteKey(String key) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        
        if (root != null && manager != null) {
            Map<String, Object> map = root.getMapForKey(key);
            if (map.remove(key) != null) {
                manager.store(map);
            }
        }
    }

    // -----------------------------------------------------------------
    // SEARCH APIs
    // -----------------------------------------------------------------
    @Override
    public List<String> getKeysByPattern(String pattern) {
        List<String> keys = new ArrayList<>();
        String prefix = pattern.endsWith("*") ? pattern.substring(0, pattern.length() - 1) : pattern;
        
        for (int shardId : assignedShards) {
            StorageRoot root = getRootForShard(shardId);
            if (root == null) continue;
            
            for (Map<String, Object> map : root.getAllMaps()) {
                for (String key : map.keySet()) {
                    if (key.startsWith(prefix)) {
                        keys.add(key);
                    }
                }
            }
        }
        return keys;
    }

    @Override
    public List<String> getKeysStartingWith(String prefix) {
        return getKeysByPattern(prefix + "*");
    }

    // -----------------------------------------------------------------
    // LOCK APIs
    // -----------------------------------------------------------------
    @Override
    public boolean acquireLock(String lockKey, String lockValue) {
        // Simple in-memory lock for now, assuming single node per shard or external coordination if needed.
        // Since EclipseStore is single-writer, we might not need complex locking if we are careful.
        // But implementing basic map-based lock for compatibility.
        int shardId = Function.calculateShardId(lockKey, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        
        if (root != null && manager != null) {
            Map<String, Object> map = root.getMapForKey(lockKey);
            if (!map.containsKey(lockKey)) {
                map.put(lockKey, lockValue);
                manager.store(map);
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean releaseLock(String lockKey, String lockValue) {
        int shardId = Function.calculateShardId(lockKey, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        
        if (root != null && manager != null) {
            Map<String, Object> map = root.getMapForKey(lockKey);
            if (lockValue.equals(map.get(lockKey))) {
                map.remove(lockKey);
                manager.store(map);
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        for (EmbeddedStorageManager manager : shardManagers.values()) {
            manager.shutdown();
        }
        logger.info("Closed all EclipseStore shards");
    }
}
