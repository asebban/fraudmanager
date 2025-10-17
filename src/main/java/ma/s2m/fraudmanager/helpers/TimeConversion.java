package ma.s2m.fraudmanager.helpers;

public class TimeConversion {

    public final static String YEAR = "YEAR";
    public final static String MONTH = "MONTH";
    public final static String DAY = "DAY";
    public final static String HOUR = "HOUR";
    public final static String MINUTE = "MINUTE";

    public Long toMillisecondes(Double value, String unit) {

        Double milliseconds;

        switch (unit) {
            case YEAR:
                milliseconds = value * 365 * 24 * 60 * 60 * 1000;
                break;
            case MONTH:
                milliseconds = value * 30 * 24 * 60 * 60 * 1000;
                break;
            case DAY:
                milliseconds = value * 24 * 60 * 60 * 1000;
                break;
            case HOUR:
                milliseconds = value * 60 * 60 * 1000;
                break;
            case MINUTE:
                milliseconds = value * 60 * 1000;
                break;
            default:
                throw new IllegalArgumentException("Invalid unit: " + unit);
        }

        return milliseconds.longValue();
    }

    public String translateTimestampToDuration(Long timestamp) {
        Long duration = timestamp;

        if (duration < 1000) {
            return duration + "ms";
        } else if (duration < 60000) {
            return duration / 1000 + "s";
        } else if (duration < 3600000) {
            return duration / 60000 + "m";
        } else if (duration < 86400000) {
            return duration / 3600000 + "h";
        } else if (duration < 2592000000L) {
            return duration / 86400000 + "d";
        } else if (duration < 31536000000L) {
            return duration / 2592000000L + "mo";
        } else {
            return duration / 31536000000L + "y";
        }
    }
    
    public static String toHumanReadableDuration(Long timestamp) {
        Long duration = timestamp;

        if (duration < 1000) {
            return duration + "ms";
        } else if (duration < 60000) {
            return duration / 1000 + "s";
        } else if (duration < 3600000) {
            return duration / 60000 + "m";
        } else if (duration < 86400000) {
            return duration / 3600000 + "h";
        } else if (duration < 2592000000L) {
            return duration / 86400000 + "d";
        } else if (duration < 31536000000L) {
            return duration / 2592000000L + "mo";
        } else {
            return duration / 31536000000L + "y";
        }
    }
}

