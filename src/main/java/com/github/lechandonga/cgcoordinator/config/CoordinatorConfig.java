package com.github.lechandonga.cgcoordinator.config;

/**
 * Coordinator configuration. Validated eagerly; invalid values are rejected
 * with {@link IllegalArgumentException} rather than silently downgraded.
 *
 * @param partitionCount total number of partitions (>= 1)
 * @param sessionTimeoutMillis session lifetime without heartbeat (>= 1)
 * @param heartbeatIntervalMillis expected client heartbeat cadence (>= 1, <= sessionTimeoutMillis)
 * @param joinTimeoutMillis how long a join may block waiting for other members (>= 1)
 * @param checkpointDir directory for durable offset checkpoints (non-null)
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
        // implemented in a later step
        throw new UnsupportedOperationException("stub");
    }
}
