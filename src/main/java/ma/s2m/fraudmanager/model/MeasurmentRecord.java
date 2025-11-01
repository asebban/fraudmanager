package ma.s2m.fraudmanager.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * This class is used to store the aggragated data of the transaction
 */
public class MeasurmentRecord implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private String valueCategory; // The valueCategory is the type of value that is being considered (Count, Amount, ...)
    private String eventObject; // The eventObject is the entity object of the event (Country, Merchant, ...)
    private String event; // The event is the operation that is being performed (Changes, Credits, Reversals, ...)
    private Long count = 0L; // The value is the value of the attribute
    private Double amount = 0.0;
    private Set<String> argSet = new LinkedHashSet<>(); // The arguments are the additional parameters that are being considered (Country, Merchant, ...)
    private List<String> argList = new ArrayList<>(); // The arguments are the additional parameters that are being considered (Country, Merchant, ...)
    private Map<String, Integer> argSetCount = new HashMap<>(); 
    private HashMap <String, Object> values = new HashMap<>(); // This hashmap records the complementary values that the user would want to add, ...
    public static final String SEPARATOR = "_";

    public MeasurmentRecord() {
    }

    public MeasurmentRecord(String valueCategory, String eventObject, String event, Set<String> arguments, long count) {
        this.valueCategory = valueCategory;
        this.eventObject = eventObject;
        this.event = event;
        if (arguments != null)
            this.argSet = arguments;
        this.count = count;
    }

    public MeasurmentRecord(String valueCategory, String eventObject, String event, Set<String> arguments, double amount) {
        this.valueCategory = valueCategory;
        this.eventObject = eventObject;
        this.event = event;
        if (arguments != null)
            this.argSet = arguments;
        this.amount = amount;
    }

	public HashMap<String, Object> getValues() {
		return values;
	}

	public void setValues(HashMap<String, Object> values) {
		this.values = values;
	}

	public void putValue(String key, Object value) {
		this.values.put(key, value);
	}

	public Object getValue(String key) {
        if (!this.values.containsKey(key) || this.values.get(key) == null) {
            return (Double)0.0;
        }

        if (this.values.get(key) instanceof Integer) {
            return ((Integer)this.values.get(key)).intValue();
        }

        if (this.values.get(key) instanceof Long) {
            return ((Long)this.values.get(key)).longValue();
        }

        if (this.values.get(key) instanceof Double) {
            return ((Double)this.values.get(key)).doubleValue();
        }

        if (this.values.get(key) instanceof Float) {
            return ((Float)this.values.get(key)).floatValue();
        }

        if (this.values.get(key) instanceof String) {
            return this.values.get(key).toString();
        }

        if (this.values.get(key) instanceof Boolean) {
            return ((Boolean)this.values.get(key)).booleanValue();
        }

        if (this.values.get(key) instanceof Short) {
            return ((Short)this.values.get(key)).shortValue();
        }

        if (this.values.get(key) instanceof Byte) {
            return ((Byte)this.values.get(key)).byteValue();
        }

        if (this.values.get(key) instanceof Character) {
            return ((Character)this.values.get(key)).charValue();
        }
        
		return this.values.get(key);
	}

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public void setDoubleValue(double doubleValue) {
        this.amount = doubleValue;
    }

    public String getValueCategory() {
        return valueCategory;
    }

    public void setValueCategory(String valueCategory) {
        this.valueCategory = valueCategory;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String operation) {
        this.event = operation;
    }

    public Set<String> getArgSet() {
        return argSet;
    }

    public void setArgSet(Set<String> arguments) {
        this.argSet = arguments;
    }

    public void addToArgSet(String argument) {
        argSetCount.put(argument, argSetCount.getOrDefault(argument, 0) + 1);
        this.argSet.add(argument);
    }

    public void removeFromArgSet(String argument) {
        if (argSetCount.containsKey(argument)) {
            int count = argSetCount.get(argument);
            if (count <= 1) {
                argSetCount.remove(argument);
                this.argSet.remove(argument);
            } else {
                argSetCount.put(argument, count - 1);
            }
        }
    }

    public void addToArgList(String argument) {
        this.argList.add(argument);
    }

    public void removeFromArgList(String argument) {
        this.argList.remove(argument);
    }

    public String getEventObject() {
        return eventObject;
    }

    public void setEventObject(String eventObject) {
        this.eventObject = eventObject;
    }

    public String key() {
        String args = valueCategory + SEPARATOR + eventObject + SEPARATOR + event;

        for (Object arg : argSet) {
            args += SEPARATOR + arg.toString();
        }

        return args;
    }

    public List<String> getArgList() {
        return argList;
    }

    public void setArgList(List<String> arguments) {
        this.argList = arguments;
    }

    public Map<String, Integer> getArgSetCount() {
        return argSetCount;
    }

    public void setArgSetCount(Map<String, Integer> argSetCount) {
        this.argSetCount = argSetCount;
    }

    @Override
    public String toString() {
        return "MR{" + "a=" + amount + ", c=" + count + '}';
    }

    @Override
    public MeasurmentRecord clone() {
        MeasurmentRecord cloned = new MeasurmentRecord();
        cloned.setValueCategory(this.valueCategory);
        cloned.setEventObject(this.eventObject);
        cloned.setEvent(this.event);
        cloned.setCount(this.count);
        cloned.setAmount(this.amount);
        cloned.setArgSet(new LinkedHashSet<>(this.argSet));
        cloned.setArgList(new ArrayList<>(this.argList));
        cloned.setArgSetCount(new HashMap<>(this.argSetCount));
        cloned.setValues(new HashMap<>(this.values));
        return cloned;
    }
}
