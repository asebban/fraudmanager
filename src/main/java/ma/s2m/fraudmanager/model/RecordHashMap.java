package ma.s2m.fraudmanager.model;

import java.util.HashMap;
import java.util.Set;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class RecordHashMap {

    private HashMap<String, MeasurmentRecord> recordHashMap;

    public RecordHashMap() {
        recordHashMap = new HashMap<>();
    }

    public RecordHashMap(String key, MeasurmentRecord value) {
        recordHashMap = new HashMap<>();
        recordHashMap.put(key, value);
    }

    public MeasurmentRecord put(String key, MeasurmentRecord value) {
        recordHashMap.put(key, value);
        return value;
    }

    public MeasurmentRecord get(String key) {
        if (recordHashMap.get(key) == null) {
            MeasurmentRecord record = new MeasurmentRecord();
            recordHashMap.put(key, record);
            return record;
        }
        else {
            return recordHashMap.get(key);
        }
    }

    public void putAll(RecordHashMap recordHashMap) {
        this.recordHashMap.putAll(recordHashMap.getRecordHashMap());
    }

    public Set<String> keySet() {
        return recordHashMap.keySet();
    }

    public int size() {
        return recordHashMap.size();
    }

    public boolean containsKey(String key) {
        return recordHashMap.containsKey(key);
    }

    public void clear() {
        recordHashMap.clear();
    }

    public MeasurmentRecord remove(String key) {
        return recordHashMap.remove(key);
    }

    public HashMap<String, MeasurmentRecord> getRecordHashMap() {
        return recordHashMap;
    }

    public void setRecordHashMap(HashMap<String, MeasurmentRecord> recordHashMap) {
        this.recordHashMap = recordHashMap;
    }

    @JsonIgnore
    public Boolean isEmpty() {
        return recordHashMap.isEmpty();
    }

    @Override
    public String toString() {
        String s = "";
        for (String key : this.keySet()) {
            s += key + " : " + this.get(key).toString() + ",";
        }
        return s;
    }
}

