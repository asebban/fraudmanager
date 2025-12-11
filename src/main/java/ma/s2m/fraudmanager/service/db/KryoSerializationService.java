package ma.s2m.fraudmanager.service.db;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;

import ma.s2m.auth.AlertSet;
import ma.s2m.auth.impl.VRTransactionSummary;
import ma.s2m.fraudmanager.model.*;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service de sérialisation/désérialisation avec Kryo pour Redis
 */
public class KryoSerializationService {

    private static final Logger logger = LoggerFactory.getLogger(KryoSerializationService.class);

    // ThreadLocal for Kryo instances (thread-safe)
    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal
            .withInitial(KryoSerializationService::createKryo);

    // ThreadLocal for Output buffer to avoid reallocation (GC reduction)
    private static final ThreadLocal<Output> outputThreadLocal = ThreadLocal
            .withInitial(() -> new Output(4096, -1)); // 4KB initial, unlimited max

    /**
     * Gets or creates a Kryo instance for the current thread
     */
    private static Kryo getKryo() {
        return kryoThreadLocal.get();
    }

    /**
     * Crée et configure une instance Kryo
     */
    private static Kryo createKryo() {
        Kryo kryo = new Kryo();

        // Configuration pour la compatibilité
        kryo.setRegistrationRequired(false);
        // Default to false for performance, but we toggle it dynamically
        kryo.setReferences(false);

        // Stratégie d'instanciation pour les objets sans constructeur par défaut
        kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

        // Enregistrement des classes principales pour de meilleures performances
        registerClasses(kryo);

        return kryo;
    }

    /**
     * Enregistre les classes principales pour optimiser la sérialisation
     */
    private static void registerClasses(Kryo kryo) {
        // Classes du domaine
        kryo.register(VRTransactionSummary.class);
        kryo.register(Measurment.class);
        kryo.register(MeasurmentRecord.class);
        kryo.register(TrxEntry.class);
        kryo.register(RecordsDelta.class);
        kryo.register(RecordHashMap.class);
        kryo.register(AlertSet.class);
        kryo.register(TrxOrAlertEvent.class);

        // Collections
        kryo.register(HashMap.class);
        kryo.register(LinkedHashMap.class);
        kryo.register(ConcurrentHashMap.class);
        kryo.register(ArrayList.class);
        kryo.register(HashSet.class);

        // Types primitifs
        kryo.register(String.class);
        kryo.register(Integer.class);
        kryo.register(Long.class);
        kryo.register(Double.class);
        kryo.register(Boolean.class);

        // Arrays
        kryo.register(String[].class);
        kryo.register(Object[].class);
    }

    /**
     * Sérialise un objet en bytes
     */
    public static byte[] serialize(Object obj) {
        if (obj == null) {
            return null;
        }

        Kryo kryo = getKryo();
        Output output = outputThreadLocal.get();
        
        try {
            output.reset(); // Reuse buffer
            // Disable references for writing to avoid IdentityObjectIntMap overhead
            kryo.setReferences(false);
            kryo.writeObject(output, obj);
            return output.toBytes();
        } catch (Exception e) {
            logger.error("Error serializing object of type {}: {}", obj.getClass().getSimpleName(), e.getMessage(), e);
            throw new RuntimeException("Kryo serialization failed", e);
        }
    }

    /**
     * Désérialise des bytes en objet du type spécifié
     */
    public static <T> T deserialize(byte[] data, Class<T> clazz) {
        if (data == null || data.length == 0) {
            return null;
        }

        Kryo kryo = getKryo();
        
        // Try with references=false (Fast path for new data)
        try {
            Input input = new Input(data);
            kryo.setReferences(false);
            return kryo.readObject(input, clazz);
        } catch (Exception e) {
            // Fallback: Try with references=true (Slow path for legacy data)
            try {
                Input input = new Input(data);
                kryo.setReferences(true);
                return kryo.readObject(input, clazz);
            } catch (Exception ex) {
                logger.error("Error deserializing data to type {}: {}", clazz.getSimpleName(), ex.getMessage(), ex);
                throw new RuntimeException("Kryo deserialization failed", ex);
            }
        }
    }

    /**
     * Sérialise une Map en bytes
     */
    public static byte[] serializeMap(Map<?, ?> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }

        Kryo kryo = getKryo();
        Output output = outputThreadLocal.get();

        try {
            output.reset();
            kryo.setReferences(false);
            kryo.writeObject(output, map);
            return output.toBytes();
        } catch (Exception e) {
            logger.error("Error serializing map: {}", e.getMessage(), e);
            throw new RuntimeException("Kryo map serialization failed", e);
        }
    }

    /**
     * Désérialise des bytes en Map<String, Measurment>
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Measurment> deserializeStringMeasurmentMap(byte[] data) {
        if (data == null || data.length == 0) {
            return new HashMap<>();
        }

        Kryo kryo = getKryo();

        try {
            Input input = new Input(data);
            kryo.setReferences(false);
            return (Map<String, Measurment>) kryo.readObject(input, HashMap.class);
        } catch (Exception e) {
            try {
                Input input = new Input(data);
                kryo.setReferences(true);
                return (Map<String, Measurment>) kryo.readObject(input, HashMap.class);
            } catch (Exception ex) {
                logger.error("Error deserializing string-measurment map: {}", ex.getMessage(), ex);
                throw new RuntimeException("Kryo map deserialization failed", ex);
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

        Kryo kryo = getKryo();

        try {
            Input input = new Input(data);
            kryo.setReferences(false);
            return (Map<Long, Measurment>) kryo.readObject(input, HashMap.class);
        } catch (Exception e) {
            try {
                Input input = new Input(data);
                kryo.setReferences(true);
                return (Map<Long, Measurment>) kryo.readObject(input, HashMap.class);
            } catch (Exception ex) {
                logger.error("Error deserializing long-measurment map: {}", ex.getMessage(), ex);
                throw new RuntimeException("Kryo map deserialization failed", ex);
            }
        }
    }

    /**
     * Cleanup method - clears the ThreadLocal to prevent memory leaks
     * Should be called when done with the current thread
     */
    public static void cleanup() {
        kryoThreadLocal.remove();
        outputThreadLocal.remove();
    }
}