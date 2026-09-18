package com.github.lechandonga.cgc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CoordinatorConfigTest {

    @TempDir
    Path dir;

    @Test
    void validConfigIsAccepted() {
        assertDoesNotThrow(() -> new CoordinatorConfig("topic", 4, 100, 1000, 2000, dir.resolve("o.log")));
    }

    @Test
    void zeroPartitionsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new CoordinatorConfig("topic", 0, 100, 1000, 2000, dir.resolve("o.log")));
    }

    @Test
    void negativeHeartbeatRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new CoordinatorConfig("topic", 4, 0, 1000, 2000, dir.resolve("o.log")));
    }

    @Test
    void sessionTimeoutBelowTwoHeartbeatsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new CoordinatorConfig("topic", 4, 100, 150, 2000, dir.resolve("o.log")));
    }

    @Test
    void rebalanceTimeoutBelowSessionTimeoutRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new CoordinatorConfig("topic", 4, 100, 1000, 500, dir.resolve("o.log")));
    }

    @Test
    void blankTopicRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new CoordinatorConfig(" ", 4, 100, 1000, 2000, dir.resolve("o.log")));
    }
}
