package ma.s2m.fraudmanager.metrics;

import io.micrometer.core.instrument.MeterRegistry;

public final class Metrics {
    private static volatile MeterRegistry registry;

    private Metrics() {}

    public static void setRegistry(MeterRegistry r) {
        registry = r;
    }

    public static MeterRegistry getRegistry() {
        return registry;
    }
}
