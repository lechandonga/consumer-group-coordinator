package com.github.lechandonga.cgc.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lechandonga.cgc.CoordinatorConfig;
import com.github.lechandonga.cgc.CoordinatorException;
import com.github.lechandonga.cgc.GroupCoordinator;
import com.github.lechandonga.cgc.HeartbeatResponse;
import com.github.lechandonga.cgc.JoinResult;
import com.github.lechandonga.cgc.JoinStatus;
import com.github.lechandonga.cgc.Partition;
import com.github.lechandonga.cgc.RejectionReason;
import com.github.lechandonga.cgc.client.CoordinatorClient;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real-TCP integration tests: clients are separate objects with separate
 * connections to a loopback server in the same JVM. They exercise the exact
 * wire path used between OS processes (marshalling, connections, concurrent
 * request handling) while staying fast and fully deterministic except for the
 * scheduler-driven timeout, which uses a wall-clock coordinator with generous
 * margins.
 */
class TcpProtocolIntegrationTest {

    @TempDir
    Path dir;

    private record Node(CoordinatorClient client, String token, int generation,
                        Set<Partition> assignment) {
    }

    private CoordinatorServer server;
    private GroupCoordinator coordinator;
    private int partitions;

    private CoordinatorServer start(int partitionCount, long sessionMs, long rebalanceMs) {
        CoordinatorConfig config = new CoordinatorConfig(
                "topic", partitionCount, 50, sessionMs, rebalanceMs,
                dir.resolve("offsets-" + System.nanoTime() + ".log"));
        coordinator = new GroupCoordinator(config);
        server = new CoordinatorServer(config, 0, coordinator);
        server.start();
        partitions = partitionCount;
        return server;
    }

    /** Full join + sync loop, waiting through IN_PROGRESS responses. */
    private Node connectAndJoin(CoordinatorClient client, String memberId, String token)
            throws Exception {
        JoinResult joined = client.joinGroup(memberId, token);
        JoinResult synced = joined;
        long guard = 0;
        while (synced.status() == JoinStatus.IN_PROGRESS && guard++ < 500) {
            Thread.sleep(5);
            synced = client.syncGroup(memberId, joined.instanceToken(), joined.generation());
        }
        assertEquals(JoinStatus.STABLE, synced.status());
        return new Node(client, synced.instanceToken(), synced.generation(), synced.assignment());
    }

    private void assertCoverage(Set<Node> nodes) {
        Map<Partition, String> owner = new HashMap<>();
        Set<String> owners = new HashSet<>();
        for (Node n : nodes) {
            owners.add(ownerName(n));
            for (Partition p : n.assignment()) {
                String prev = owner.put(p, ownerName(n));
                assertFalse(prev != null && !prev.equals(ownerName(n)),
                        () -> "partition " + p + " owned by both " + prev + " and " + ownerName(n));
            }
        }
        assertEquals(partitions, owner.size(), "complete coverage");
        assertEquals(nodes.size(), owners.size());
        assertEquals(partitions, coordinator.ownershipSnapshot().size());
    }

    private String ownerName(Node n) {
        return n.token(); // unique per live instance
    }

    @Test
    void multipleConnectionsJoinSharePartitionsAndCommit() throws Exception {
        start(4, 5_000, 10_000);
        try (CoordinatorClient ca = new CoordinatorClient("127.0.0.1", server.port());
             CoordinatorClient cb = new CoordinatorClient("127.0.0.1", server.port())) {
            ca.ping();
            Node a = connectAndJoin(ca, "a", null);
            // Bring b in; both members must finish phase 2 over the wire.
            JoinResult bJoin = cb.joinGroup("b", null);
            assertEquals(JoinStatus.IN_PROGRESS, bJoin.status());
            ca.joinGroup("a", a.token()); // a phase 1 (window stays open)
            JoinResult bWaiting = cb.syncGroup("b", bJoin.instanceToken(), bJoin.generation());
            assertEquals(JoinStatus.IN_PROGRESS, bWaiting.status(),
                    "b alone cannot close the window before a finishes phase 2");
            // a finishes phase 2 -> window closes; b then syncs and gets stable.
            JoinResult aSynced = ca.syncGroup("a", a.token(), bJoin.generation());
            assertEquals(JoinStatus.STABLE, aSynced.status());
            JoinResult bSynced = cb.syncGroup("b", bJoin.instanceToken(), bJoin.generation());
            assertEquals(JoinStatus.STABLE, bSynced.status());
            Node aStable = new Node(ca, a.token(), aSynced.generation(), aSynced.assignment());
            Node b = new Node(cb, bSynced.instanceToken(), bSynced.generation(),
                    bSynced.assignment());
            assertCoverage(Set.of(aStable, b));
            assertEquals(2, aStable.assignment().size());
            assertEquals(2, b.assignment().size());

            // Both commit their own partitions.
            for (Partition p : aStable.assignment()) {
                ca.commit("a", aStable.token(), aStable.generation(), p, 1);
                assertEquals(1, ca.fetchCommitted(p));
            }
            // Cross-ownership commit is rejected over the wire.
            Partition foreign = b.assignment().iterator().next();
            CoordinatorException e = assertThrows(CoordinatorException.class,
                    () -> ca.commit("a", aStable.token(), aStable.generation(), foreign, 1));
            assertEquals(RejectionReason.NOT_OWNER, e.reason());
        } finally {
            server.close();
        }
    }

    private Node connectAndJoinSync(CoordinatorClient client, String memberId, JoinResult joined)
            throws Exception {
        JoinResult synced = joined;
        long guard = 0;
        while (synced.status() == JoinStatus.IN_PROGRESS && guard++ < 500) {
            Thread.sleep(5);
            synced = client.syncGroup(memberId, joined.instanceToken(), joined.generation());
        }
        return new Node(client, synced.instanceToken(), synced.generation(), synced.assignment());
    }

    @Test
    void heartbeatKeepsAssignmentAndFetchesCommitted() throws Exception {
        start(2, 5_000, 10_000);
        try (CoordinatorClient ca = new CoordinatorClient("127.0.0.1", server.port())) {
            Node a = connectAndJoin(ca, "a", null);
            HeartbeatResponse hb = ca.heartbeat("a", a.token(), a.generation());
            assertEquals(a.generation(), hb.generation());
            assertEquals(a.assignment(), hb.assignment());
            ca.commit("a", a.token(), a.generation(), new Partition("topic", 0), 9);
            assertEquals(9, ca.fetchCommitted(new Partition("topic", 0)));
            assertEquals(-1, ca.fetchCommitted(new Partition("topic", 1)));
        } finally {
            server.close();
        }
    }

    @Test
    void restartedInstanceTakesOverAndOldConnectionIsFenced() throws Exception {
        start(3, 5_000, 10_000);
        try (CoordinatorClient oldConn = new CoordinatorClient("127.0.0.1", server.port());
             CoordinatorClient newConn = new CoordinatorClient("127.0.0.1", server.port())) {
            Node old = connectAndJoin(oldConn, "a", null);
            Partition p0 = new Partition("topic", 0);
            oldConn.commit("a", old.token(), old.generation(), p0, 4);

            // New process, same member id, blank token -> takeover.
            JoinResult neoJoin = newConn.joinGroup("a", null);
            assertNotEquals(old.token(), neoJoin.instanceToken());
            Node neo = connectAndJoinSync(newConn, "a", neoJoin);
            assertTrue(neo.generation() > old.generation());
            assertEquals(3, neo.assignment().size());

            // Old connection keeps trying on another socket: fenced every time.
            CoordinatorException hb = assertThrows(CoordinatorException.class,
                    () -> oldConn.heartbeat("a", old.token(), neo.generation()));
            assertEquals(RejectionReason.FENCED_INSTANCE, hb.reason());
            CoordinatorException commit = assertThrows(CoordinatorException.class,
                    () -> oldConn.commit("a", old.token(), neo.generation(), p0, 5));
            assertEquals(RejectionReason.FENCED_INSTANCE, commit.reason());

            // Old instance's attempts changed nothing; new instance progresses.
            assertEquals(4, newConn.fetchCommitted(p0));
            newConn.commit("a", neo.token(), neo.generation(), p0, 5);
            assertEquals(5, newConn.fetchCommitted(p0));

            // Presenting a bogus token is fenced as well.
            CoordinatorException bogus = assertThrows(CoordinatorException.class,
                    () -> oldConn.heartbeat("a", "inst-deadbeef", neo.generation()));
            assertEquals(RejectionReason.FENCED_INSTANCE, bogus.reason());
        } finally {
            server.close();
        }
    }

    @Test
    void inProgressObservableOverWireAndTimeoutClosesDeterministically() throws Exception {
        // rebalance bound 400ms; sessions long so only the bound matters.
        start(4, 30_000, 400);
        try (CoordinatorClient ca = new CoordinatorClient("127.0.0.1", server.port());
             CoordinatorClient cb = new CoordinatorClient("127.0.0.1", server.port())) {
            Node a = connectAndJoin(ca, "a", null);
            JoinResult bJoin = cb.joinGroup("b", null);
            assertEquals(JoinStatus.IN_PROGRESS, bJoin.status());

            long t0 = System.currentTimeMillis();
            // b observes IN_PROGRESS (distinct from an error) while a is silent.
            JoinResult waiting = cb.syncGroup("b", bJoin.instanceToken(), bJoin.generation());
            assertEquals(JoinStatus.IN_PROGRESS, waiting.status());

            // Wait for the bound to elapse: b's next sync closes with the flag.
            JoinResult closed;
            do {
                Thread.sleep(20);
                closed = cb.syncGroup("b", bJoin.instanceToken(), bJoin.generation());
            } while (closed.status() == JoinStatus.IN_PROGRESS
                    && System.currentTimeMillis() - t0 < 5_000);
            assertEquals(JoinStatus.STABLE, closed.status());
            assertTrue(closed.completedByTimeout(), "must be flagged as timeout completion");
            assertTrue(System.currentTimeMillis() - t0 >= 350,
                    "must not close materially before the configured bound");

            // a never participated: evicted; b owns the full set, no overlap.
            assertEquals(Set.of("b"), coordinator.members());
            Node b = new Node(cb, closed.instanceToken(), closed.generation(), closed.assignment());
            assertCoverage(Set.of(b));
            assertEquals(4, b.assignment().size());
            assertTrue(coordinator.verifyNoOwnershipOverlap());

            // a's old connection keeps trying after being evicted: it is fenced
            // (not unknown, not stale), so it cannot re-occupy or commit.
            Node aFinal = a;
            JoinResult closedFinal = closed;
            CoordinatorException oldHb = assertThrows(CoordinatorException.class,
                    () -> ca.heartbeat("a", aFinal.token(), closedFinal.generation()));
            assertEquals(RejectionReason.FENCED_INSTANCE, oldHb.reason());
            CoordinatorException oldCommit = assertThrows(CoordinatorException.class,
                    () -> ca.commit("a", aFinal.token(), closedFinal.generation(),
                            new Partition("topic", 0), 123));
            assertEquals(RejectionReason.FENCED_INSTANCE, oldCommit.reason());
        } finally {
            server.close();
        }
    }

    @Test
    void unknownMemberAndOffsetRegressionOverWire() throws Exception {
        start(1, 5_000, 10_000);
        try (CoordinatorClient ca = new CoordinatorClient("127.0.0.1", server.port())) {
            Node a = connectAndJoin(ca, "a", null);
            Partition p0 = new Partition("topic", 0);
            ca.commit("a", a.token(), a.generation(), p0, 10);
            CoordinatorException reg = assertThrows(CoordinatorException.class,
                    () -> ca.commit("a", a.token(), a.generation(), p0, 3));
            assertEquals(RejectionReason.OFFSET_REGRESSION, reg.reason());
            CoordinatorException ghost = assertThrows(CoordinatorException.class,
                    () -> ca.heartbeat("ghost", "inst-x", a.generation()));
            assertEquals(RejectionReason.UNKNOWN_MEMBER, ghost.reason());
            assertFalse(a.assignment().isEmpty());
        } finally {
            server.close();
        }
    }
}
