package ma.s2m.fraudmanager.service.processors;

import java.util.function.Function;

import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;

import ma.s2m.fraudmanager.service.db.IStoreService;

public class KeyProcessor {

    private IStoreService storageService;
    Logger logger = org.slf4j.LoggerFactory.getLogger(KeyProcessor.class);

    /**
     * Striped locks to guarantee mutual exclusion per key inside a single JVM.
     * This fixes races under load even when the underlying store lock APIs are no-ops.
     */
    private static final int LOCK_STRIPES = 65536;
    private final ReentrantLock[] stripedLocks = new ReentrantLock[LOCK_STRIPES];

    // Log lock contention (wait time) to help diagnose latency under load.
    // Tunable via JVM system properties (milliseconds).
    private static final long LOCK_WAIT_INFO_MS = Long.getLong("app.lock.wait.info.ms", 25L);
    private static final long LOCK_WAIT_WARN_MS = Long.getLong("app.lock.wait.warn.ms", 250L);


    public KeyProcessor(IStoreService storageService) {
        this.storageService = storageService;

        for (int i = 0; i < LOCK_STRIPES; i++) {
            stripedLocks[i] = new ReentrantLock();
        }
    }

    private ReentrantLock lockForKey(String key) {
        int h = key != null ? key.hashCode() : 0;
        // Spread bits to reduce stripe collisions for similar hash codes.
        h ^= (h >>> 16);

        // Fast path when LOCK_STRIPES is a power of two (it is: 65536).
        if ((LOCK_STRIPES & (LOCK_STRIPES - 1)) == 0) {
            return stripedLocks[h & (LOCK_STRIPES - 1)];
        }

        @SuppressWarnings("unused")
        int idx = Math.floorMod(h, LOCK_STRIPES);
        return stripedLocks[idx];
    }
    /**
     * Exécute une opération avec verrouillage automatique
     * @param dataKey La clé des données à traiter
     * @param processor Fonction qui traite les données
     * @return Le résultat du traitement ou null en cas d'échec
     */
    public <T> T executeWithLock(String dataKey, Function<String, T> processor) {

        if (dataKey == null) {
            logger.error("executeWithLock called with null dataKey");
            return null;
        }
        if (processor == null) {
            logger.error("executeWithLock called with null processor for key: {}", dataKey);
            return null;
        }

        ReentrantLock lock = lockForKey(dataKey);

        final long waitStartNs = System.nanoTime();
        lock.lock();
        final long waitedMs = (System.nanoTime() - waitStartNs) / 1_000_000L;
        try {
            // Splitter la dataKey avec séparateur ":" et prendre l'élément d'index 1
            int index = dataKey.indexOf(":");
            String keyElement = index > 0 ? dataKey.substring(index + 1) : dataKey;

            if (waitedMs >= LOCK_WAIT_WARN_MS) {
                logger.warn("Lock contention: waited {} ms for key {} (dataKey={})", waitedMs, keyElement, dataKey);
            } else if (waitedMs >= LOCK_WAIT_INFO_MS) {
                logger.info("Lock contention: waited {} ms for key {} (dataKey={})", waitedMs, keyElement, dataKey);
            }

            logger.debug("Processing datakey: {} -> key: {}", dataKey, keyElement);
            return processor.apply(keyElement);
        } catch (Exception e) {
            logger.error("Error processing data for key: {}", dataKey, e);
            return null;
        } finally {
            lock.unlock();
        }
    }

}
