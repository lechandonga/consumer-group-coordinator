package com.github.lechandonga.cgcoordinator.client;

import com.github.lechandonga.cgcoordinator.assign.RangeAssignor;
import com.github.lechandonga.cgcoordinator.audit.OwnershipAuditLog;
import com.github.lechandonga.cgcoordinator.config.CoordinatorConfig;
import com.github.lechandonga.cgcoordinator.group.GroupCoordinator;
import com.github.lechandonga.cgcoordinator.model.CommitResult;
import com.github.lechandonga.cgcoordinator.net.CoordinatorServer;
import com.github.lechandonga.cgcoordinator.offset.FileOffsetStore;
import com.github.lechandonga.cgcoordinator.reject.CoordinatorException;
import com.github.lechandonga.cgcoordinator.reject.RejectReason;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerClientTest {

    @TempDir
    Path dir;

    private CoordinatorServer server;
    private int port;

    @BeforeEach
    void setUp() {
        CoordinatorConfig cfg = new CoordinatorConfig(3, 5_000, 1_000, 800, dir.toString());
        GroupCoordinator c = new GroupCoordinator(cfg, new RangeAssignor(),
                new FileOffsetStore(dir.toString(), 3), new OwnershipAuditLog());
        server = new CoordinatorServer(c, 0, 50);
        server.start();
        port = server.boundPort();
    }

    @AfterEach
    void tearDown() {
        server.close();
    }

    @Test
    void joinReturnsGenerationPartitionsAndOffsets() {
        try (ConsumerClient c = new ConsumerClient("127.0.0.1", port, "a")) {
            List<Integer> parts = c.join(2_000);
            assertTrue(c.generation() >= 1);
            assertEquals(List.of(0, 1, 2), parts);
            assertEquals(-1L, c.fetchOffset(0));
        }
    }

    @Test
    void eachClientGetsUniqueIncarnation() {
        try (ConsumerClient c1 = new ConsumerClient("127.0.0.1", port, "a");
             ConsumerClient c2 = new ConsumerClient("127.0.0.1", port, "a")) {
            assertNotEquals(c1.incarnationId(), c2.incarnationId());
        }
    }

    @Test
    void heartbeatRenewsSession() {
        try (ConsumerClient c = new ConsumerClient("127.0.0.1", port, "a")) {
            c.join(2_000);
            assertEquals("STABLE", c.heartbeatOnce());
        }
    }

    @Test
    void commitAcceptedThenFetched() {
        try (ConsumerClient c = new ConsumerClient("127.0.0.1", port, "a")) {
            c.join(2_000);
            CommitResult r = c.commit(1, 7);
            assertTrue(r.accepted());
            assertEquals(7L, c.fetchOffset(1));
        }
    }

    @Test
    void zombieIncarnationCommitComesBackAsRejectedResult() throws Exception {
        String inc1;
        try (ConsumerClient c1 = new ConsumerClient("127.0.0.1", port, "a")) {
            c1.join(2_000);
            inc1 = c1.incarnationId();
        }
        try (ConsumerClient c2 = new ConsumerClient("127.0.0.1", port, "a")) {
            c2.join(2_000);
            long gen = c2.generation();
            // hand-craft a zombie commit using c2's socket but old incarnation
            CommitResult r = server.getCoordinator()
                    .commit("a", inc1, gen, 0, 1);
            assertFalse(r.accepted());
            assertEquals("FENCED_INCARNATION", r.rejectReason());
        }
    }

    @Test
    void joinOfBlankMemberRejectedLocally() {
        assertThrows(IllegalArgumentException.class,
                () -> new ConsumerClient("127.0.0.1", port, " "));
    }
}
