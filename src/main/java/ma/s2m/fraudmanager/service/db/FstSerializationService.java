package ma.s2m.fraudmanager.service.db;

import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ma.s2m.auth.AlertSet;
import ma.s2m.auth.impl.VRTransactionSummary;
import ma.s2m.fraudmanager.model.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service de sérialisation/désérialisation avec FST (Fast Serialization)
 * Remplaçant performant pour Kryo.
 */
public class FstSerializationService {

    private static final Logger logger = LoggerFactory.getLogger(FstSerializationService.class);

    // FSTConfiguration est thread-safe
    private static final FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    static {
        // Configuration pour la performance (pas de références partagées, comme pour Kryo)
        conf.setShareReferences(false);
        // Allow serialization of classes that do not implement Serializable
        conf.setForceSerializable(true);

        // Enregistrement des classes (optionnel mais recommandé pour la perf)
        registerClasses();
    }

    private static void registerClasses() {
        conf.registerClass(
                VRTransactionSummary.class,
                Measurment.class,
                MeasurmentRecord.class,
                TrxEntry.class,
                RecordsDelta.class,
                RecordHashMap.class,
                AlertSet.class,
                TrxOrAlertEvent.class,
                HashMap.class,
                LinkedHashMap.class,
                ConcurrentHashMap.class,
                ArrayList.class,
                HashSet.class,
                String[].class,
                Object[].class
        );
    }

    /**
     * Sérialise un objet en bytes
     */
    public static byte[] serialize(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            return conf.asByteArray(obj);
        } catch (Exception e) {
            logger.error("Error serializing object of type {}: {}", obj.getClass().getSimpleName(), e.getMessage(), e);
            throw new RuntimeException("FST serialization failed", e);
        }
    }

    /**
     * Désérialise des bytes en objet du type spécifié
     * Tente FST d'abord, puis fallback sur Kryo pour la compatibilité ascendante.
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserialize(byte[] data, Class<T> clazz) {
        if (data == null || data.length == 0) {
            return null;
        }

        try {
            // Tentative de désérialisation FST
            return (T) conf.asObject(data);
        } catch (Exception e) {
            // Fallback : Tentative avec Kryo (pour les anciennes données)
            try {
                return KryoSerializationService.deserialize(data, clazz);
            } catch (Exception ex) {
                logger.error("Error deserializing data to type {} (FST and Kryo failed): {}", clazz.getSimpleName(), ex.getMessage(), ex);
                throw new RuntimeException("Deserialization failed", ex);
            }
        }
    }

    /**
     * Sérialise une Map en bytes
     */
    public static byte[] serializeMap(Map<?, ?> map) {
        return serialize(map);
    }

    /**
     * Désérialise des bytes en Map<String, Measurment>
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Measurment> deserializeStringMeasurmentMap(byte[] data) {
        if (data == null || data.length == 0) {
            return new HashMap<>();
        }
        try {
            return (Map<String, Measurment>) conf.asObject(data);
        } catch (Exception e) {
            try {
                return KryoSerializationService.deserializeStringMeasurmentMap(data);
            } catch (Exception ex) {
                logger.error("Error deserializing string-measurment map: {}", ex.getMessage(), ex);
                throw new RuntimeException("Map deserialization failed", ex);
            }
        }
    }

    /**
     * Désérialise des bytes en Map<Long, Measurment>
     */
    @SuppressWarnings("unchecked")
    public static Map<Long, Measurment> deserializeLongMeasurmentMap(byte[] data) {
        if (data == null || data.length == 0) {
            return new HashMap<>();
        }
        try {
            return (Map<Long, Measurment>) conf.asObject(data);
        } catch (Exception e) {
            try {
                return KryoSerializationService.deserializeLongMeasurmentMap(data);
            } catch (Exception ex) {
                logger.error("Error deserializing long-measurment map: {}", ex.getMessage(), ex);
                throw new RuntimeException("Map deserialization failed", ex);
            }
        }
    }
}
