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
    void rebalanceTimeoutBelowTwoHeartbeatsRejected() {
        // heartbeat=100 -> rebalanceTimeout must be >= 200 and is independent
        // of sessionTimeout (here 1000), so a small bound with a long session
        // is legal while a sub-two-heartbeat bound is rejected.
        assertThrows(IllegalArgumentException.class,
                () -> new CoordinatorConfig("topic", 4, 100, 1000, 150, dir.resolve("o.log")));
    }

    @Test
    void rebalanceTimeoutMayBeSmallerThanSessionTimeout() {
        // Short rebalance bound, long session: independently tunable.
        assertDoesNotThrow(
                () -> new CoordinatorConfig("topic", 4, 100, 1000, 200, dir.resolve("o2.log")));
    }

    @Test
    void blankTopicRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new CoordinatorConfig(" ", 4, 100, 1000, 2000, dir.resolve("o.log")));
    }
}
