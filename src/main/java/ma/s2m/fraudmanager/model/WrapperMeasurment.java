package ma.s2m.fraudmanager.model;

public class WrapperMeasurment {

    private Measurment measurment;
    private Long windowStartTime;
    private Long windowEndTime;

    public WrapperMeasurment(Measurment measurment) {
        this.measurment = measurment;
    }

    public WrapperMeasurment(Measurment measurment, Long windowStartTime, Long windowEndTime) {
        this.measurment = measurment;
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
    }

    public Measurment getMeasurment() {
        return measurment;
    }
    public void setMeasurment(Measurment measurment) {
        this.measurment = measurment;
    }
    public Long getWindowStartTime() {
        return windowStartTime;
    }
    public void setWindowStartTime(Long windowStartTime) {
        this.windowStartTime = windowStartTime;
    }
    public Long getWindowEndTime() {
        return windowEndTime;
    }
    public void setWindowEndTime(Long windowEndTime) {
        this.windowEndTime = windowEndTime;
    }

    public static WrapperMeasurment createNewWrapperMeasurment(Measurment measurment, Long trxTimestamp) {
        WrapperMeasurment wm =  new WrapperMeasurment(measurment);
        Long remaining = trxTimestamp % measurment.getWindowSize();
        Long startTimestamp = trxTimestamp - remaining;
        Long endTimestamp = startTimestamp + measurment.getWindowSize();
        wm.setWindowStartTime(startTimestamp);
        wm.setWindowEndTime(endTimestamp);
        return wm;
    }
}
