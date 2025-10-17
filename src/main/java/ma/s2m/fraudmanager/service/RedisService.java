package ma.s2m.fraudmanager.service;

import ma.s2m.fraudmanager.model.Measurment;
import ma.s2m.fraudmanager.util.RetryUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RedisService {
    private static final Logger logger = LoggerFactory.getLogger(RedisService.class);
    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final ObjectMapper mapper = new ObjectMapper();
    
    // Constantes pour le verrouillage
    private static final int LOCK_TIMEOUT_SECONDS = 30; // Timeout du verrou
            
    // Script Lua pour acquérir un verrou
    private static final String ACQUIRE_LOCK_SCRIPT = """
            if redis.call('exists', KEYS[1]) == 0 then
                redis.call('setex', KEYS[1], ARGV[1], ARGV[2])
                return 1
            else
                return 0
            end
            """;
            
    // Script Lua pour libérer un verrou
    private static final String RELEASE_LOCK_SCRIPT = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end
            """;

    public RedisService(RedisClient client) {
        this.client = client;
        this.connection = client.connect();
    }

    /**
     * Lit une clé contenant une Map de Measurments et la désérialise
     * @param key La clé à lire
     * @return Map<Long, Measurment> ou une map vide si la clé n'existe pas
     */
    public Map<Long, Measurment> getMeasurments(String key) {
        return RetryUtil.retry(() -> {
            RedisCommands<String, String> sync = connection.sync();
            String json = sync.get(key);
            try {
                if (json != null) {
                    TypeReference<Map<Long, Measurment>> typeRef = new TypeReference<Map<Long, Measurment>>() {};
                    Map<Long, Measurment> measurments = mapper.readValue(json, typeRef);
                    logger.debug("Read {} windows of measurments from key: {}", measurments.size(), key);
                    return measurments;
                } else {
                    logger.debug("Key {} not found, returning empty map", key);
                    return new HashMap<>();
                }
            } catch (JsonProcessingException e) {
                logger.error("Error deserializing measurments map for key: {}", key, e);
                return new HashMap<>();
            }
        });
    }

    /**
     * Sauvegarde une Map de Measurments dans Redis après sérialisation
     * @param key La clé où sauvegarder la map
     * @param measurments La map de measurments à sauvegarder
     */
    public void setMeasurments(String key, Map<Long, Measurment> measurments) {
        RetryUtil.retry(() -> {
            try {
                RedisCommands<String, String> sync = connection.sync();
                String json = mapper.writeValueAsString(measurments);
                sync.set(key, json);
                logger.debug("Saved {} measurments to key: {}", measurments.size(), key);
            } catch (JsonProcessingException e) {
                logger.error("Error serializing measurments map for key: {}", key, e);
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Acquiert un verrou distribué sur une clé
     * @param lockKey La clé du verrou
     * @param lockValue Valeur unique pour identifier le propriétaire du verrou
     * @return true si le verrou a été acquis, false sinon
     */
    public boolean acquireLock(String lockKey, String lockValue) {
        try {
            RedisCommands<String, String> sync = connection.sync();
            String[] keys = {lockKey};
            String[] args = {String.valueOf(LOCK_TIMEOUT_SECONDS), lockValue};
            
            Long result = sync.eval(ACQUIRE_LOCK_SCRIPT, ScriptOutputType.INTEGER, keys, args);
            boolean acquired = result != null && result == 1;
            
            if (acquired) {
                logger.debug("Lock acquired for key: {}", lockKey);
            }
            
            return acquired;
        } catch (Exception e) {
            logger.error("Error acquiring lock for key: {}", lockKey, e);
            return false;
        }
    }

    /**
     * Libère un verrou distribué
     * @param lockKey La clé du verrou
     * @param lockValue Valeur unique pour identifier le propriétaire du verrou
     * @return true si le verrou a été libéré, false sinon
     */
    public boolean releaseLock(String lockKey, String lockValue) {
        try {
            RedisCommands<String, String> sync = connection.sync();
            String[] keys = {lockKey};
            String[] args = {lockValue};
            
            Long result = sync.eval(RELEASE_LOCK_SCRIPT, ScriptOutputType.INTEGER, keys, args);
            boolean released = result != null && result == 1;
            
            if (released) {
                logger.debug("Lock released for key: {}", lockKey);
            } else {
                logger.warn("Failed to release lock for key: {} (lock not owned by this instance)", lockKey);
            }
            
            return released;
        } catch (Exception e) {
            logger.error("Error releasing lock for key: {}", lockKey, e);
            return false;
        }
    }

    // Fermeture graceful
    public void close() {
        connection.close();
        client.shutdown();
    }
}