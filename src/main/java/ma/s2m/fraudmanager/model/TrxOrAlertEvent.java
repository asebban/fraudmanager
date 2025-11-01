package ma.s2m.fraudmanager.model;

import ma.s2m.auth.AlertSet;
import ma.s2m.auth.impl.VRTransactionSummary;

public class TrxOrAlertEvent implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private VRTransactionSummary transaction;
    private AlertSet alertSet = new AlertSet();
    private Long timestamp;
    private String correlationId;

    public TrxOrAlertEvent(VRTransactionSummary transaction, AlertSet alertSet, String correlationId) {
        this.transaction = transaction;
        this.alertSet = alertSet;
        this.timestamp = System.currentTimeMillis();
        this.correlationId = correlationId;
    }

    public TrxOrAlertEvent(VRTransactionSummary transaction, AlertSet alertSet, Long timestamp, String correlationId) {
        this.transaction = transaction;
        this.alertSet = alertSet;
        this.timestamp = timestamp;
        this.correlationId = correlationId;
    }

    public TrxOrAlertEvent(TrxOrAlertEvent other) {
        this.timestamp = other.getTimestamp();
        this.correlationId = other.getCorrelationId();
        // copie profonde de la transaction
        this.transaction = new VRTransactionSummary(other.getTransaction()); // Assuming VRTransactionSummary has a copy constructor
        // copie profonde de l'alertSet
        this.alertSet = other.getAlertSet() != null ? other.getAlertSet().copy() : null;
        // copier aussi les autres champs de previousEvent...
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long ts) {
        this.timestamp = ts;
    }

    public VRTransactionSummary getTransaction() {
        return transaction;
    }

    public void setTransaction(VRTransactionSummary transaction) {
        this.transaction = transaction;
    }

    public AlertSet getAlertSet() {
        return alertSet;
    }

    public void setAlertSet(AlertSet alertSet) {
        this.alertSet = alertSet;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }
}
