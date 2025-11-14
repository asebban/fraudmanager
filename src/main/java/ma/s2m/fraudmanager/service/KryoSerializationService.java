package ma.s2m.fraudmanager.service;

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service de sérialisation/désérialisation avec Kryo pour Redis
 */
public class KryoSerializationService {
    
    private static final Logger logger = LoggerFactory.getLogger(KryoSerializationService.class);
    
    // ScopedValue for Kryo instances (virtual thread compatible)
    private static final ScopedValue<Kryo> kryoScoped = ScopedValue.newInstance();
    
    /**
     * Gets or creates a Kryo instance for the current scope
     */
    private static Kryo getKryo() {
        Kryo kryo = kryoScoped.orElse(null);
        if (kryo == null) {
            kryo = createKryo();
        }
        return kryo;
    }
    
    /**
     * Crée et configure une instance Kryo
     */
    private static Kryo createKryo() {
        Kryo kryo = new Kryo();
        
        // Configuration pour la compatibilité
        kryo.setRegistrationRequired(false);
        kryo.setReferences(true);
        
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
        
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             Output output = new Output(baos)) {
            
            Kryo kryo = getKryo();
            kryo.writeObject(output, obj);
            output.flush();
            
            return baos.toByteArray();
            
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
        
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             Input input = new Input(bais)) {
            
            Kryo kryo = getKryo();
            return kryo.readObject(input, clazz);
            
        } catch (Exception e) {
            logger.error("Error deserializing data to type {}: {}", clazz.getSimpleName(), e.getMessage(), e);
            throw new RuntimeException("Kryo deserialization failed", e);
        }
    }
    
    /**
     * Sérialise une Map en bytes
     */
    public static byte[] serializeMap(Map<?, ?> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }
        
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             Output output = new Output(baos)) {
            
            Kryo kryo = getKryo();
            kryo.writeObject(output, map);
            output.flush();
            
            return baos.toByteArray();
            
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
        
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             Input input = new Input(bais)) {
            
            Kryo kryo = getKryo();
            Object result = kryo.readObject(input, HashMap.class);
            
            return (Map<String, Measurment>) result;
            
        } catch (Exception e) {
            logger.error("Error deserializing string-measurment map: {}", e.getMessage(), e);
            throw new RuntimeException("Kryo map deserialization failed", e);
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
        
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             Input input = new Input(bais)) {
            
            Kryo kryo = getKryo();
            Object result = kryo.readObject(input, HashMap.class);
            
            return (Map<Long, Measurment>) result;
            
        } catch (Exception e) {
            logger.error("Error deserializing long-measurment map: {}", e.getMessage(), e);
            throw new RuntimeException("Kryo map deserialization failed", e);
        }
    }
    
    /**
     * Cleanup method - no longer needed with ScopedValue
     * Kept for backward compatibility but does nothing
     */
    public static void cleanup() {
        // No-op: ScopedValue automatically cleans up when scope ends
    }
}