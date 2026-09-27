package com.github.lechandonga.cgc;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Coordinator configuration. Invalid values are rejected eagerly in the
 * constructor instead of being silently degraded.
 */
public record CoordinatorConfig(
        String topic,
        int partitionCount,
        long heartbeatIntervalMs,
        long sessionTimeoutMs,
        long rebalanceTimeoutMs,
        Path offsetStorePath,
        RebalanceMode rebalanceMode) {

    /** Backward-compatible constructor: {@link RebalanceMode#IMMEDIATE}. */
    public CoordinatorConfig(String topic, int partitionCount, long heartbeatIntervalMs,
                             long sessionTimeoutMs, long rebalanceTimeoutMs, Path offsetStorePath) {
        this(topic, partitionCount, heartbeatIntervalMs, sessionTimeoutMs, rebalanceTimeoutMs,
                offsetStorePath, RebalanceMode.IMMEDIATE);
    }

    public CoordinatorConfig {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(offsetStorePath, "offsetStorePath");
        Objects.requireNonNull(rebalanceMode, "rebalanceMode");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be >= 1, got " + partitionCount);
        }
        if (heartbeatIntervalMs < 1) {
            throw new IllegalArgumentException("heartbeatIntervalMs must be >= 1, got " + heartbeatIntervalMs);
        }
        if (sessionTimeoutMs < 2 * heartbeatIntervalMs) {
            throw new IllegalArgumentException(
                    "sessionTimeoutMs (" + sessionTimeoutMs + ") must be >= 2 * heartbeatIntervalMs ("
                            + heartbeatIntervalMs + ")");
        }
        if (rebalanceTimeoutMs < sessionTimeoutMs) {
            throw new IllegalArgumentException(
                    "rebalanceTimeoutMs (" + rebalanceTimeoutMs + ") must be >= sessionTimeoutMs ("
                            + sessionTimeoutMs + ")");
        }
    }

    public static CoordinatorConfig of(int partitionCount, Path offsetStorePath) {
        return new CoordinatorConfig("topic", partitionCount, 100, 1000, 2000, offsetStorePath);
    }
}
