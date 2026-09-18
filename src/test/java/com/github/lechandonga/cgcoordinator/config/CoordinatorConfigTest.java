package com.github.lechandonga.cgcoordinator.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorConfigTest {

    @Test
    void validConfigAccepted() {
        assertDoesNotThrow(() -> new CoordinatorConfig(8, 10_000, 3_000, 2_000, "./data"));
    }

    @Test
    void zeroPartitionsRejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new CoordinatorConfig(0, 10_000, 3_000, 2_000, "./data"));
        assertTrue(e.getMessage().contains("partitionCount"));
    }

    @Test
    void negativeTimeoutsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new CoordinatorConfig(8, 0, 3_000, 2_000, "./data"));
        assertThrows(IllegalArgumentException.class,
                () -> new CoordinatorConfig(8, 10_000, 0, 2_000, "./data"));
        assertThrows(IllegalArgumentException.class,
                () -> new CoordinatorConfig(8, 10_000, 3_000, 0, "./data"));
    }

    @Test
    void heartbeatLargerThanSessionRejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new CoordinatorConfig(8, 1_000, 5_000, 500, "./data"));
        assertTrue(e.getMessage().contains("heartbeatIntervalMillis"));
    }

    @Test
    void blankCheckpointDirRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new CoordinatorConfig(8, 10_000, 3_000, 2_000, "  "));
        assertThrows(IllegalArgumentException.class,
                () -> new CoordinatorConfig(8, 10_000, 3_000, 2_000, null));
    }
}
