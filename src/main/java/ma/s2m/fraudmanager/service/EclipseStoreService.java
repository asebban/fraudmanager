package ma.s2m.fraudmanager.service;

import ma.medtech.droolbuilder.rules.Subject;
import ma.s2m.fraudmanager.config.AppConfig;
import ma.s2m.fraudmanager.model.Measurment;
import ma.s2m.fraudmanager.model.RecordHashMap;
import ma.s2m.fraudmanager.model.WrapperMeasurment;
import ma.s2m.functions.Function;
import ma.s2m.fraudmanager.metrics.Metrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.LongAdder;

public class EclipseStoreService implements IStoreService {
    private static final Logger logger = LoggerFactory.getLogger(EclipseStoreService.class);
    
    // Pour simplifier et optimiser, nous allons utiliser un verrou par SHARD au lieu d'un verrou par clé
    // C'est la granularité la plus logique pour la persistance EclipseStore.
    private final Map<Integer, ReentrantLock> shardLocks = new HashMap<>(); 

    // Shard configuration
    private final int totalDiskShards;
    private final List<Integer> assignedShards;

    // EclipseStore instances per shard
    private final Map<Integer, EmbeddedStorageManager> shardManagers = new HashMap<>();
    private final Map<Integer, StorageRoot> shardRoots = new HashMap<>();

    private final ConcurrentHashMap<Integer, Boolean> dirtyShards = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> flushTask;

    // Fréquence de persistance (configurable via AppConfig)
    private final long flushIntervalMs;    

    // Metrics basiques
    private final LongAdder flushCount = new LongAdder();
    private final LongAdder flushedShardBatches = new LongAdder();
    private final LongAdder flushDurationNs = new LongAdder();

    // Micrometer meters (optional, when registry available)
    private Counter flushCounter;
    private Counter flushShardBatchesCounter;
    private Timer flushTimer;

    public EclipseStoreService(String dbPath) {
        this.totalDiskShards = AppConfig.storageDiskShardCount;
        this.assignedShards = new ArrayList<>(AppConfig.storageShards);
        this.flushIntervalMs = AppConfig.storageFlushIntervalMs;

        if (this.assignedShards.isEmpty()) {
            logger.warn("No shards assigned to this node, defaulting to shard 0");
            this.assignedShards.add(0);
        }

        logger.info(AppConfig.nodeName + ": Initializing EclipseStore with {} total disk shards, assigned shards: {}",
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
            // Initialiser l'état dirty pour le shard
            dirtyShards.put(shardId, false);
            logger.info(AppConfig.nodeName + ": Successfully opened EclipseStore shard {}", shardId);
        }

        // Initialisation du thread de nettoyage
        startFlushScheduler();

        // Register Micrometer meters if registry is present
        tryRegisterMeters();
    }

        private void tryRegisterMeters() {
        MeterRegistry reg = Metrics.getRegistry();
        if (reg == null) return;
        flushCounter = Counter.builder("eclipsestore.flush.count")
            .description("Number of async flush executions")
            .tag("node", AppConfig.nodeName)
            .register(reg);
        flushShardBatchesCounter = Counter.builder("eclipsestore.flush.shards")
            .description("Total shards flushed across all executions")
            .tag("node", AppConfig.nodeName)
            .register(reg);
        flushTimer = Timer.builder("eclipsestore.flush.duration")
            .description("Duration of async flush execution")
            .publishPercentileHistogram()
            .tag("node", AppConfig.nodeName)
            .register(reg);
        }

    private void startFlushScheduler() {
        Runnable flusher = new Runnable() {
            @Override
            public void run() {
                long start = System.nanoTime();
                try {
                    int batches = flushDirtyShards();
                    long dur = System.nanoTime() - start;
                    flushCount.increment();
                    flushedShardBatches.add(batches);
                    flushDurationNs.add(dur);
                    if (flushCounter != null) {
                        flushCounter.increment();
                    }
                    if (flushShardBatchesCounter != null && batches > 0) {
                        flushShardBatchesCounter.increment(batches);
                    }
                    if (flushTimer != null) {
                        flushTimer.record(dur, java.util.concurrent.TimeUnit.NANOSECONDS);
                    }
                    // Log léger toutes les ~200 exécutions
                    long count = flushCount.sum();
                    if (count % 200 == 0) {
                        double avgMs = (flushDurationNs.sum() / (double) count) / 1_000_000.0;
                        double avgBatches = flushedShardBatches.sum() / (double) count;
                        logger.debug(AppConfig.nodeName + ": EclipseStore flush stats: executions={} avgDurationMs={} avgShardsPerFlush={}", count, String.format("%.3f", avgMs), String.format("%.2f", avgBatches));
                    }
                } catch (Exception e) {
                    logger.error(AppConfig.nodeName + ": Erreur lors du flush asynchrone des shards.", e);
                }
            }
        };
        
        // Planifie la tâche de flush avec un délai fixe afin d'éviter l'effet rattrapage
        flushTask = scheduler.scheduleWithFixedDelay(flusher,
                                                    flushIntervalMs,
                                                    flushIntervalMs,
                                                    TimeUnit.MILLISECONDS);
        logger.info(AppConfig.nodeName + ": Scheduler de flush EclipseStore démarré (Délai fixe: {} ms)", flushIntervalMs);
    }

    private int flushDirtyShards() {
        int flushed = 0;
        for (int shardId : assignedShards) {
            // Vérifie si le shard a été modifié depuis le dernier flush
            if (Boolean.TRUE.equals(dirtyShards.get(shardId))) {
                ReentrantLock shardLock = getLockForShard(shardId);
                EmbeddedStorageManager manager = getManagerForShard(shardId);
                
                if (manager != null && shardLock != null) {
                    shardLock.lock();
                    try {
                        manager.store(shardRoots.get(shardId).getAllMaps()); // Commit toutes les modifications en mémoire
                        dirtyShards.put(shardId, false); // Réinitialise l'état
                        flushed++;
                    } finally {
                        shardLock.unlock();
                    }
                }
            }
        }
        return flushed;
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
                // Passage à l'écriture asynchrone pour être cohérent avec les autres méthodes
                dirtyShards.put(shardId, true);
            } finally {
                shardLock.unlock();
            }
        }
    }

    @Override
    public void setRecordHashMapByKey(String key, RecordHashMap recordHashMap) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        ReentrantLock shardLock = getLockForShard(shardId);
        
        if (root != null && manager != null && shardLock != null) {
            shardLock.lock(); 
            try {
                Map<String, Object> map = root.getMapForKey(key);
                map.put(key, recordHashMap);
                dirtyShards.put(shardId, true);
            } finally {
                shardLock.unlock();
            }
        }
    }

    @Override
    public void setMeasurmentByKey(String key, Measurment measurment) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        ReentrantLock shardLock = getLockForShard(shardId);
        
        if (root != null && manager != null && shardLock != null) {
            shardLock.lock(); 
            try {
                Map<String, Object> map = root.getMapForKey(key);
                map.put(key, measurment);
                dirtyShards.put(shardId, true);
            } finally {
                shardLock.unlock();
            }
        }
    }

    @Override
    public void setWrapperMeasurmentByKey(String key, WrapperMeasurment wrapperMeasurment) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        ReentrantLock shardLock = getLockForShard(shardId);
        
        if (root != null && manager != null && shardLock != null) {
            shardLock.lock(); 
            try {
                Map<String, Object> map = root.getMapForKey(key);
                map.put(key, wrapperMeasurment);
                dirtyShards.put(shardId, true);
            } finally {
                shardLock.unlock();
            }
        }
    }

    // La méthode flushBatch() reste un no-op ou peut appeler flushDirtyShards() immédiatement    @Override
    public void flushBatch() {
        flushDirtyShards();
    }

    @Override
    public void deleteKey(String key) {
        int shardId = Function.calculateShardId(key, totalDiskShards);
        StorageRoot root = getRootForShard(shardId);
        EmbeddedStorageManager manager = getManagerForShard(shardId);
        ReentrantLock shardLock = getLockForShard(shardId);
        
        if (root != null && manager != null && shardLock != null) {
            shardLock.lock(); 
            try {
                Map<String, Object> map = root.getMapForKey(key);
                if (map.remove(key) != null) {
                    dirtyShards.put(shardId, true);
                }
            } finally {
                shardLock.unlock();
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
        // Tentative de flush final des shards modifiés avant arrêt
        try {
            flushDirtyShards();
        } catch (Exception e) {
            logger.warn("Flush final avant arrêt a échoué", e);
        }

        if (flushTask != null) {
            flushTask.cancel(false); // Arrêter la planification sans interrompre la tâche en cours
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Le scheduler de flush ne s'est pas terminé dans le délai imparti. Forcing shutdown now.");
                scheduler.shutdownNow();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }

        // Double sécurité: si des writes se sont produits après le dernier flush, refait un flush par shard
        for (int shardId : assignedShards) {
            if (Boolean.TRUE.equals(dirtyShards.get(shardId))) {
                ReentrantLock shardLock = getLockForShard(shardId);
                EmbeddedStorageManager manager = getManagerForShard(shardId);
                if (manager != null && shardLock != null) {
                    shardLock.lock();
                    try {
                        manager.store(shardRoots.get(shardId).getAllMaps());
                        dirtyShards.put(shardId, false);
                    } finally {
                        shardLock.unlock();
                    }
                }
            }
        }

        for (EmbeddedStorageManager manager : shardManagers.values()) {
            manager.shutdown();
        }
        double avgMs = (flushCount.sum() == 0) ? 0.0 : (flushDurationNs.sum() / (double) flushCount.sum()) / 1_000_000.0;
        double avgBatches = (flushCount.sum() == 0) ? 0.0 : flushedShardBatches.sum() / (double) flushCount.sum();
        logger.info("Closed all EclipseStore shards. Flush stats: executions={} avgDurationMs={} avgShardsPerFlush={}",
                flushCount.sum(), String.format("%.3f", avgMs), String.format("%.2f", avgBatches));
    }
}