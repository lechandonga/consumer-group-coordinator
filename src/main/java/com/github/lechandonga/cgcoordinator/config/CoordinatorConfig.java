package com.github.lechandonga.cgcoordinator.config;

/**
 * Coordinator configuration. Validated eagerly; invalid values are rejected
 * with {@link IllegalArgumentException} rather than silently downgraded.
 *
 * <p>Behavior for a given configuration is deterministic and reproducible:
 * the assignor is deterministic; the reaper uses the configured session
 * timeout; a rebalance collects joining members for a fixed
 * {@code joinTimeoutMillis} before finalizing, so a burst of nearly
 * simultaneous joins is handled by exactly one rebalance regardless of
 * scheduling jitter.
 *
 * @param partitionCount total number of partitions ({@code >= 1})
 * @param sessionTimeoutMillis session lifetime without a heartbeat ({@code >= 1})
 * @param heartbeatIntervalMillis expected client heartbeat cadence
 *                                ({@code >= 1} and {@code <= sessionTimeoutMillis})
 * @param joinTimeoutMillis fixed member-collection window of a rebalance,
 *                          also the upper bound a join blocks ({@code >= 1})
 * @param checkpointDir directory for durable offset checkpoints (non-blank)
 */
public record CoordinatorConfig(int partitionCount,
                                long sessionTimeoutMillis,
                                long heartbeatIntervalMillis,
                                long joinTimeoutMillis,
                                String checkpointDir) {

    public CoordinatorConfig {
        validate(partitionCount, sessionTimeoutMillis, heartbeatIntervalMillis,
                joinTimeoutMillis, checkpointDir);
    }

    static void validate(int partitionCount, long sessionTimeoutMillis,
                         long heartbeatIntervalMillis, long joinTimeoutMillis,
                         String checkpointDir) {
        if (partitionCount < 1) {
            throw new IllegalArgumentException(
                    "partitionCount must be >= 1 but was " + partitionCount);
        }
        if (sessionTimeoutMillis < 1L) {
            throw new IllegalArgumentException(
                    "sessionTimeoutMillis must be >= 1 but was " + sessionTimeoutMillis);
        }
        if (heartbeatIntervalMillis < 1L) {
            throw new IllegalArgumentException(
                    "heartbeatIntervalMillis must be >= 1 but was " + heartbeatIntervalMillis);
        }
        if (heartbeatIntervalMillis > sessionTimeoutMillis) {
            throw new IllegalArgumentException(
                    "heartbeatIntervalMillis (" + heartbeatIntervalMillis
                            + ") must be <= sessionTimeoutMillis (" + sessionTimeoutMillis + ")");
        }
        if (joinTimeoutMillis < 1L) {
            throw new IllegalArgumentException(
                    "joinTimeoutMillis must be >= 1 but was " + joinTimeoutMillis);
        }
        if (checkpointDir == null || checkpointDir.isBlank()) {
            throw new IllegalArgumentException("checkpointDir must not be blank");
        }
    }
}
