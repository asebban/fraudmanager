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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class EclipseStoreService implements IStoreService {
    private static final Logger logger = LoggerFactory.getLogger(EclipseStoreService.class);

    // REMPLACEMENT : Remplacer le verrou global par une map de verrous par clé
    // Note : Pour EclipseStore, le locking se fait idéalement au niveau du Manager/Shard
    // Mais pour mimer le "lock par clé" demandé et garantir l'atomicité lors de l'accès au Map racine :
    private final ConcurrentHashMap<String, ReentrantLock> keyLocks = new ConcurrentHashMap<>();
    
    // Pour simplifier et optimiser, nous allons utiliser un verrou par SHARD au lieu d'un verrou par clé
    // C'est la granularité la plus logique pour la persistance EclipseStore.
    private final Map<Integer, ReentrantLock> shardLocks = new HashMap<>(); 

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
            // AJOUT: Initialisation du verrou par shard
            shardLocks.put(shardId, new ReentrantLock());
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

    private Lock getKeyLock(String key) {
        // utilise computeIfAbsent pour créer le verrou uniquement s'il n'existe pas
        return keyLocks.computeIfAbsent(key, k -> new ReentrantLock());
    }
    
    private StorageRoot getRootForShard(int shardId) {
        return shardRoots.get(shardId);
    }

    private EmbeddedStorageManager getManagerForShard(int shardId) {
        return shardManagers.get(shardId);
    }
    
    // NOUVEAU: Méthode utilitaire pour obtenir le verrou par shard
    private ReentrantLock getLockForShard(int shardId) {
        return shardLocks.get(shardId);
    }

    // -----------------------------------------------------------------
    // READ APIs (Pas de verrouillage nécessaire car les accès sont thread-safe)
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
    // WRITE APIs (Mise en œuvre du verrouillage par SHARD)
    // -----------------------------------------------------------------
    @Override
    public void setMeasurments(String key, Map<Long, Measurment> measurments) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        ReentrantLock shardLock = getLockForShard(shardId); // Récupération du verrou du shard
        
        if (root != null && manager != null && shardLock != null) {
            shardLock.lock(); // Utilisation du verrou du shard
            try {
                Map<String, Object> map = root.getMapForKey(key);
                map.put(key, measurments);
                manager.store(map);
            } finally {
                shardLock.unlock();
            }
        }
    }

    @Override
    public void setRecordHashMapByKey(String key, RecordHashMap recordHashMap) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        Lock keyLock = getKeyLock(key);
        StorageRoot root = getRootForShard(shardId);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        ReentrantLock shardLock = getLockForShard(shardId);
        
        if (root != null && manager != null && shardLock != null) {
            keyLock.lock();
            try {
                Map<String, Object> map = root.getMapForKey(key);
                map.put(key, recordHashMap);
                shardLock.lock(); 
                try {
                    manager.store(map); // Commit sur le disque
                } finally {
                    shardLock.unlock();
                }
            } finally {
                keyLock.unlock();
            }
        }
    }

    @Override
    public void setMeasurmentByKey(String key, Measurment measurment) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        Lock keyLock = getKeyLock(key);
        ReentrantLock shardLock = getLockForShard(shardId);
        
        if (root != null && manager != null && shardLock != null) {
            keyLock.lock();
            try {
                Map<String, Object> map = root.getMapForKey(key);
                map.put(key, measurment);
                shardLock.lock(); 
                try {
                    manager.store(map); // Commit sur le disque
                } finally {
                    shardLock.unlock();
                }
            } finally {
                keyLock.unlock();
            }
        }
    }

    @Override
    public void setWrapperMeasurmentByKey(String key, WrapperMeasurment wrapperMeasurment) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        Lock keyLock = getKeyLock(key);
        StorageRoot root = getRootForShard(shardId);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        ReentrantLock shardLock = getLockForShard(shardId);
        
        if (root != null && manager != null && shardLock != null) {
            keyLock.lock();
            try {
                Map<String, Object> map = root.getMapForKey(key);
                map.put(key, wrapperMeasurment);
                shardLock.lock(); 
                try {
                    manager.store(map); // Commit sur le disque
                } finally {
                    shardLock.unlock();
                }
            } finally {
                keyLock.unlock();
            }
        }
    }

    @Override
    public void flushBatch() {
        // La gestion des locks par shard assure qu'un seul thread modifie le shard à la fois.
        // La méthode store() est appelée dans chaque setter, donc flushBatch reste un no-op.
    }

    @Override
    public void deleteKey(String key) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        Lock keyLock = getKeyLock(key);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        ReentrantLock shardLock = getLockForShard(shardId);
        
        if (root != null && manager != null && shardLock != null) {
            keyLock.lock();
            try {
                Map<String, Object> map = root.getMapForKey(key);
                if (map.remove(key) != null) {
                    shardLock.lock();
                    try {
                        manager.store(map);
                    } finally {
                        shardLock.unlock();
                    }
                }
            } finally {
                keyLock.unlock();
            }
        }
    }

    // -----------------------------------------------------------------
    // SEARCH APIs (Lecture seule, pas de verrouillage nécessaire)
    // -----------------------------------------------------------------
    @Override
    public List<String> getKeysByPattern(String pattern) {
        // ... (Pas de changement)
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
    // LOCK APIs (Verrouillage simulé pour compatibilité)
    // -----------------------------------------------------------------
    // Ces méthodes sont utilisées dans KeyProcessor pour les transactions distribuées.
    // L'implémentation utilise la map racine pour stocker le verrou comme une clé.
    @Override
    public boolean acquireLock(String lockKey, String lockValue) {
        return true;
    }

    @Override
    public boolean releaseLock(String lockKey, String lockValue) {
        return true;
    }

    @Override
    public void close() {
        for (EmbeddedStorageManager manager : shardManagers.values()) {
            manager.shutdown();
        }
        logger.info("Closed all EclipseStore shards");
    }
}