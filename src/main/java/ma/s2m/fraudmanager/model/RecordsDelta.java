package ma.s2m.fraudmanager.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RecordsDelta implements Serializable {
    private static final long serialVersionUID = 1L;

    private long countDelta;
    private double amountDelta;
    private Map<String, Object> valuesDelta;
    private Set<String> argSetDelta;
    private List<String> argListDelta;

    public long getCountDelta() { return countDelta; }
    public void setCountDelta(long countDelta) { this.countDelta = countDelta; }

    public double getAmountDelta() { return amountDelta; }
    public void setAmountDelta(double amountDelta) { this.amountDelta = amountDelta; }

    public Map<String, Object> getValuesDelta() {
        if (valuesDelta == null) valuesDelta = new HashMap<>();
        return valuesDelta;
    }
    public void setValuesDelta(Map<String, Object> valuesDelta) { this.valuesDelta = valuesDelta; }

    @JsonIgnore
    public boolean empty() {
        return countDelta == 0 && amountDelta == 0.0 && (valuesDelta == null || valuesDelta.isEmpty() && (argSetDelta == null || argSetDelta.isEmpty()) && (argListDelta == null || argListDelta.isEmpty()));
    }

    public Set<String> getArgSetDelta() { return argSetDelta; }
    public void setArgSetDelta(Set<String> argSetDelta) { this.argSetDelta = argSetDelta; }

    public List<String> getArgListDelta() { return argListDelta; }
    public void setArgListDelta(List<String> argListDelta) { this.argListDelta = argListDelta; }

    @Override
    public String toString() {
        return "RD [c=" + countDelta + ", a=" + amountDelta + "]";
    }
}
