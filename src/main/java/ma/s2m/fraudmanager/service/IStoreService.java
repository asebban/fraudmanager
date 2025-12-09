package ma.s2m.fraudmanager.service;

import ma.s2m.fraudmanager.model.Measurment;
import ma.s2m.fraudmanager.model.RecordHashMap;
import ma.s2m.fraudmanager.model.WrapperMeasurment;

import java.util.List;
import java.util.Map;

public interface IStoreService {

    // Read APIs
    Map<Long, Measurment> getMeasurments(String key);
    RecordHashMap getRecordHashMapByKey(String key);
    Measurment getMeasurmentByKey(String key);
    WrapperMeasurment getWrapperMeasurmentByKey(String key);

    // Write APIs
    void setMeasurments(String key, Map<Long, Measurment> measurments);
    void setRecordHashMapByKey(String key, RecordHashMap recordHashMap);
    void setMeasurmentByKey(String key, Measurment measurment);
    void setWrapperMeasurmentByKey(String key, WrapperMeasurment wrapperMeasurment);

    // Batch operations
    void flushBatch();

    // Delete API
    void deleteKey(String key);

    // Search APIs
    List<String> getKeysByPattern(String pattern);
    List<String> getKeysStartingWith(String prefix);

    // Lock APIs
    boolean acquireLock(String lockKey, String lockValue);
    boolean releaseLock(String lockKey, String lockValue);

    // Lifecycle
    void close();
}
