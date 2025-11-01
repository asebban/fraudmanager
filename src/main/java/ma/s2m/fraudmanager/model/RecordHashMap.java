package ma.s2m.fraudmanager.model;

import java.util.HashMap;
import java.util.Set;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class RecordHashMap implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

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

    /** Lecture SANS création (ne modifie rien) */
    public MeasurmentRecord peek(String key) {
        return recordHashMap.get(key);
    }

    /** Vue non modifiable pour itération sûre */
    public HashMap<String, MeasurmentRecord> asUnmodifiableMap() {
        return (HashMap<String, MeasurmentRecord>) java.util.Collections.unmodifiableMap(recordHashMap);
    }

    /** Snapshot (copie superficielle des entrées) pour itérations à l’abri des mutations */
    public HashMap<String, MeasurmentRecord> snapshot() {
        return new HashMap<>(recordHashMap);
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

