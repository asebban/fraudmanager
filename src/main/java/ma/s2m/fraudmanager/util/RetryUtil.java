package ma.s2m.fraudmanager.util;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;

import java.time.Duration;
import java.util.function.Supplier;

public class RetryUtil {
    private static final Retry retry = Retry.of("default", RetryConfig.custom()
            .maxAttempts(3)
            .waitDuration(Duration.ofMillis(500))
            .build());

    public static <T> T retry(Supplier<T> supplier) {
        return retry.executeSupplier(supplier);
    }

    public static void retry(Runnable runnable) {
        retry.executeRunnable(runnable);
    }
}