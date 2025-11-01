package ma.s2m.fraudmanager.service;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;

public class KeyProcessor {

    private RocksDBService rocksDBService;

    // Constantes pour le verrouillage
    private static final int LOCK_RETRY_DELAY_MS = 10;  // Délai entre les tentatives
    private static final int MAX_LOCK_RETRIES = 100;    // Nombre max de tentatives
    Logger logger = org.slf4j.LoggerFactory.getLogger(KeyProcessor.class);


    public KeyProcessor(RocksDBService rocksDBService) {
        this.rocksDBService = rocksDBService;
    }
    /**
     * Exécute une opération avec verrouillage automatique
     * @param dataKey La clé des données à traiter
     * @param processor Fonction qui traite les données
     * @return Le résultat du traitement ou null en cas d'échec
     */
    public <T> T executeWithLock(String dataKey, Function<String, T> processor) {
        String lockKey = "lock:" + dataKey;
        String lockValue = UUID.randomUUID().toString();
        
        // Tentatives d'acquisition du verrou
        int retries = 0;
        while (retries < MAX_LOCK_RETRIES) {
            if (rocksDBService.acquireLock(lockKey, lockValue)) {
                try {
                    // Splitter la dataKey avec séparateur ":" et prendre l'élément d'index 1
                    int index = dataKey.indexOf(":");
                    String keyElement = index > 0 ? dataKey.substring(index + 1) : dataKey;
                    logger.debug("Processing datakey: {} -> key: {}", dataKey, keyElement);
                    
                    // Traiter les données
                    T result = processor.apply(keyElement);
                    return result;
                    
                } catch (Exception e) {
                    logger.error("Error processing data for key: {}", dataKey, e);
                    return null;
                } finally {
                    // Toujours libérer le verrou
                    rocksDBService.releaseLock(lockKey, lockValue);
                }
            }
            else {
                // Attendre avant de réessayer
                try {
                    TimeUnit.MILLISECONDS.sleep(LOCK_RETRY_DELAY_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Interrupted while waiting for lock: {}", lockKey);
                    return null;
                }
                
                retries++;
            }
            
            
        }
        
        logger.error("Failed to acquire lock for key: {} after {} retries", dataKey, MAX_LOCK_RETRIES);
        return null;
    }

}
