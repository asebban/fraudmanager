package ma.s2m.fraudmanager.service;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Global, thread-safe flag used to temporarily short-circuit processing while
 * a new rules version is being deployed/reloaded.
 */
public final class RuleDeploymentState {

    private static final AtomicBoolean inProgress = new AtomicBoolean(false);
    private static final AtomicReference<String> requestedVersion = new AtomicReference<>(null);

    private RuleDeploymentState() {
    }

    /**
     * Mark deployment as in progress.
     *
     * @param version version label to expose to clients (may be null/blank)
     * @return true if this call transitioned the state from not-in-progress to in-progress
     */
    public static boolean begin(String version) {
        if (version != null && !version.isBlank()) {
            requestedVersion.set(version.trim());
        }
        return inProgress.compareAndSet(false, true);
    }

    public static void end() {
        inProgress.set(false);
        requestedVersion.set(null);
    }

    public static boolean isInProgress() {
        return inProgress.get();
    }

    public static String getRequestedVersionOrUnknown() {
        String v = requestedVersion.get();
        return (v == null || v.isBlank()) ? "unknown" : v;
    }

    public static void updateRequestedVersion(String version) {
        if (version != null && !version.isBlank()) {
            requestedVersion.set(version.trim());
        }
    }
}
