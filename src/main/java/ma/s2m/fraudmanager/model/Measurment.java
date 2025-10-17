package ma.s2m.fraudmanager.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.core.factmodel.traits.Traitable;
import org.kie.api.definition.type.PropertyReactive;


import ma.s2m.auth.impl.VRTransactionSummary;
import ma.s2m.auth.AlertSet;

@PropertyReactive
@Traitable
public class Measurment implements Cloneable {

	private Long window;
	private Long windowSize;
	private VRTransactionSummary transaction;
	private String subject;	//The subject is the entity that is being tracked (Card, Merchant, ...)
	private List<TrxEntry> trxEntries = new ArrayList<>(); // This list records all the transactions that have been processed in this measurment.

	private boolean applyAlerts = true;
	private String key;

	private RecordHashMap records = new RecordHashMap(); // This list records all the rule data for the current transaction.
	private HashMap<String, Object> lasts = new HashMap<>(); // This hashmap records the last value of attributes for the concerned subject (Country, merchant, ...).
	private HashMap<String, Integer> lastsCount = new HashMap<>(); // This hashmap records the count of occurrences of the same attributes

	private AlertSet alertSet = new AlertSet();

	private Boolean dirty = false; // This is used to indicate if the measurment has been modified since the last time it was processed.

	public Measurment() {
	}

	public Measurment(String key) {
		this.key = key;
	}

	public Measurment(String key, Long window) {
		this.key = key;
		this.window = window;
	}

	public List<TrxEntry> getTrxEntries() {
		return trxEntries;
	}

	public void setTrxEntries(List<TrxEntry> trxEntries) {
		this.trxEntries.clear();
		this.trxEntries.addAll(trxEntries);
	}

	public void addTransaction(TrxEntry trxEntry) {
		this.trxEntries.add(trxEntry);
	}

	public void removeTransaction(TrxEntry trxEntry) {
		this.trxEntries.remove(trxEntry);
	}

	public boolean expired(TrxEntry trxEntry, Long windowSize) {
		Long now = System.currentTimeMillis();
		return (now - trxEntry.getEventTimeMs()) > windowSize;
	}

	public Measurment clone() {
		Measurment clone = new Measurment();
		clone.setWindow(this.window);
		clone.setKey(this.getKey());
		clone.setWindowSize(this.windowSize);
		clone.setTransaction(this.transaction);

		// Perform a deep copy of the RecordHashMap
		RecordHashMap clonedRecords = new RecordHashMap();
		for (Map.Entry<String, MeasurmentRecord> entry : this.getRecords().getRecordHashMap().entrySet()) {
			clonedRecords.getRecordHashMap().put(entry.getKey(), entry.getValue().clone());
		}
		clone.setRecords(clonedRecords);

		HashMap<String, Object> clonedLasts = new HashMap<>();
		for (Map.Entry<String, Object> entry : this.getLasts().entrySet()) {
			clonedLasts.put(entry.getKey(), entry.getValue());
		}
		this.lasts = clonedLasts;

		clone.setLastsCount(new HashMap<>(this.lastsCount));
		
		clone.setAlertSet(this.alertSet);
		clone.setDirty(this.dirty);
		return clone;
	}

	private void setLastsCount(HashMap<String, Integer> hashmap) {
		this.lastsCount = hashmap;
	}

	public AlertSet getAlertSet() {
		return alertSet;
	}

	public void setAlertSet(AlertSet alertSet) {
		this.alertSet = alertSet;
	}

	public Measurment add(Measurment m) {

		// Ensure the lists of records are compatible in size
		if (this.records.size() != m.records.size()) {
			throw new IllegalArgumentException("The records lists must be of the same size to perform addition.");
		}

		// Iterate through the records and add the values
		for (String key : this.records.keySet()) {
			MeasurmentRecord recordFromMe = this.records.get(key);
			MeasurmentRecord recordFromOther = m.records.get(key);
			if (recordFromOther != null && recordFromMe != null) {
				recordFromMe.setCount(recordFromMe.getCount() + recordFromOther.getCount());
				recordFromMe.setDoubleValue(recordFromMe.getAmount() + recordFromOther.getAmount());
			}
			this.getRecords().put(key, recordFromMe);
		}
	
		return this;
	}

	public Long getWindowSize() {
		return windowSize;
	}

	public void setWindowSize(Long windowSize) {
		this.windowSize = windowSize;
	}

	public Long getWindow() {
		return window;
	}
	public void setWindow(Long window) {
		this.window = window;
	}

	public VRTransactionSummary getTransaction() {
		return transaction;
	}

	public void setTransaction(VRTransactionSummary transaction) {
		this.transaction = transaction;
	}

	public RecordHashMap getRecords() {
		return records;
	}

	public void setRecords(RecordHashMap record) {
		this.records = record;
	}

	public HashMap<String, Object> getLasts() {
		return lasts;
	}

	public void addToLast(String key, Object value) {
		this.lasts.put(key, value);
		this.lastsCount.put(key, this.lastsCount.getOrDefault(key, 0) + 1);
	}

	public void removeFromLast(String key) {
		if (this.lastsCount.containsKey(key)) {
			int count = this.lastsCount.get(key);
			if (count <= 1) {
				this.lasts.remove(key);
				this.lastsCount.remove(key);
			} else {
				this.lastsCount.put(key, count - 1);
			}
		}
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Boolean getDirty() {
		return dirty;
	}

	public void setDirty(Boolean dirty) {
		this.dirty = dirty;
	}

	public Boolean isDirty() {
		return dirty;
	}

	@Override
	public String toString() {
		return "Measurment [window=" + window + ", windowSize=" + windowSize + ", transaction=" + transaction
				+ ", subject=" + subject + ", applyAlerts=" + applyAlerts + ", key=" + key + ", records=" + records
				+ ", lasts=" + lasts + ", alerts=" + alertSet + "]";
	}

}

