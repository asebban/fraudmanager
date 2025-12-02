package ma.s2m.fraudmanager.model;

import java.io.Serializable;
import java.util.Map;

import ma.s2m.auth.impl.VRTransactionSummary;

public class TrxEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    private VRTransactionSummary tx;
    private String txNo;
    private Long eventTimeMs;
    private Map<String, RecordsDelta> recordDelta;
    private Map<String, Object> lastsDelta;

    public VRTransactionSummary getTx() { return tx; }
    public void setTx(VRTransactionSummary tx) { this.tx = tx; }

    public Long getEventTimeMs() { return eventTimeMs != null ? eventTimeMs : System.currentTimeMillis(); }
    public void setEventTimeMs(Long eventTimeMs) { this.eventTimeMs = eventTimeMs; }

    public Map<String, RecordsDelta> getRecordDelta() { return recordDelta; }
    public void setRecordDelta(Map<String, RecordsDelta> delta) { this.recordDelta = delta; }

    public Map<String, Object> getLastsDelta() { return lastsDelta; }
    public void setLastsDelta(Map<String, Object> lastsDelta) { this.lastsDelta = lastsDelta; }

    public String getTxNo() { return txNo; }
    public void setTxNo(String txNo) { this.txNo = txNo; }
}
