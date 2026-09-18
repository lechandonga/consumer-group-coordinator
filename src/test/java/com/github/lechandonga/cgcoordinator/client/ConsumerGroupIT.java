package com.github.lechandonga.cgcoordinator.client;

import com.github.lechandonga.cgcoordinator.assign.RangeAssignor;
import com.github.lechandonga.cgcoordinator.audit.OwnershipAuditLog;
import com.github.lechandonga.cgcoordinator.config.CoordinatorConfig;
import com.github.lechandonga.cgcoordinator.group.GroupCoordinator;
import com.github.lechandonga.cgcoordinator.model.CommitResult;
import com.github.lechandonga.cgcoordinator.net.CoordinatorServer;
import com.github.lechandonga.cgcoordinator.offset.FileOffsetStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end scenarios over real loopback sockets with independent
 * {@link ConsumerClient} instances (each gets a unique incarnation id, just
 * like a separate OS process).
 */
class ConsumerGroupIT {

    @TempDir
    Path dir;

    private OwnershipAuditLog audit;
    private GroupCoordinator coordinator;
    private CoordinatorServer server;
    private int port;

    @BeforeEach
    void setUp() {
        CoordinatorConfig cfg = new CoordinatorConfig(8, 1_500, 500, 800, dir.toString());
        audit = new OwnershipAuditLog();
        coordinator = new GroupCoordinator(cfg, new RangeAssignor(),
                new FileOffsetStore(dir.toString(), 8), audit);
        server = new CoordinatorServer(coordinator, 0, 50);
        server.start();
        port = server.boundPort();
    }

    @AfterEach
    void tearDown() {
        server.close();
    }

    /** Re-join with the same client until STABLE and all expected members present. */
    private void stabilize(Set<String> members, Map<String, ConsumerClient> clients)
            throws Exception {
        for (int round = 0; round < 8; round++) {
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(members.size());
            List<Future<?>> fs = new java.util.ArrayList<>();
            for (String m : members) {
                ConsumerClient c = clients.get(m);
                fs.add(pool.submit(() -> {
                    start.await();
                    try {
                        c.join(3_000);
                    } catch (RuntimeException re) {
                        // missed join window; the outer loop re-joins
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : fs) {
                f.get(6, TimeUnit.SECONDS);
            }
            pool.shutdown();
            if (coordinator.state() == com.github.lechandonga.cgcoordinator.group.GroupState.STABLE
                    && new HashSet<String>(coordinator.snapshot().members())
                            .equals(new HashSet<String>(members))) {
                pool.shutdownNow();
                return;
            }
        }
        throw new AssertionError("failed to stabilize " + members
            + " last=" + coordinator.snapshot().state()
            + coordinator.snapshot().members());
    }

    @Test
    void concurrentJoinRebalanceCoversAllPartitionsExactlyOnce() throws Exception {
        Map<String, ConsumerClient> clients = new HashMap<>();
        Set<String> members = Set.of("a", "b", "c");
        for (String m : members) {
            clients.put(m, new ConsumerClient("127.0.0.1", port, m));
        }
        try {
            stabilize(members, clients);
            long gen = coordinator.generation();
            Set<Integer> all = new HashSet<>();
            for (String m : members) {
                ConsumerClient c = clients.get(m);
                assertEquals(gen, c.generation());
                for (int p : c.partitions()) {
                    assertTrue(all.add(p), "partition " + p + " double-assigned");
                }
            }
            assertEquals(Set.of(0, 1, 2, 3, 4, 5, 6, 7), all);
            assertEquals(1, coordinator.ownersOf(3).size());
            assertFalse(audit.hasSimultaneousOverlap());
        } finally {
            clients.values().forEach(ConsumerClient::close);
        }
    }

    @Test
    void memberFailureTriggersRebalanceAndNoPartitionIsLeftOrDoublyOwned() throws Exception {
        Map<String, ConsumerClient> clients = new HashMap<>();
        Set<String> members = new HashSet<>(Set.of("a", "b"));
        for (String m : members) {
            clients.put(m, new ConsumerClient("127.0.0.1", port, m));
        }
        try {
            stabilize(members, clients);
            long before = coordinator.generation();
            // b "crashes": close without leave and stop heartbeating.
            clients.get("b").close();
            clients.remove("b");
            members.remove("b");

            // wait for the server reaper to expire b's session
            long deadline = System.currentTimeMillis() + 8_000;
            while (System.currentTimeMillis() < deadline
                    && coordinator.snapshot().members().contains("b")) {
                Thread.sleep(100);
            }
            assertFalse(coordinator.snapshot().members().contains("b"),
                    "dead member must be expired");
            stabilize(members, clients);
            assertTrue(coordinator.generation() > before);
            assertEquals(Set.of("a"), new HashSet<>(coordinator.snapshot().members()));
            assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7),
                    coordinator.snapshot().assignments().get("a"));
            assertFalse(audit.hasSimultaneousOverlap());
        } finally {
            clients.values().forEach(ConsumerClient::close);
        }
    }

    @Test
    void staleGenerationAndNonOwnerCommitsAreRejectedAndDoNotMutateState() throws Exception {
        Map<String, ConsumerClient> clients = new HashMap<>();
        Set<String> members = new HashSet<>(Set.of("a", "b"));
        for (String m : members) {
            clients.put(m, new ConsumerClient("127.0.0.1", port, m));
        }
        try {
            stabilize(members, clients);
            long gen = coordinator.generation();
            ConsumerClient a = clients.get("a");
            ConsumerClient b = clients.get("b");

            // Pick a partition owned by b.
            int bPartition = b.partitions().get(0);
            assertFalse(a.partitions().contains(bPartition));

            CommitResult notOwner = a.commit(bPartition, 1);
            assertFalse(notOwner.accepted());
            assertEquals("NOT_OWNER", notOwner.rejectReason());
            assertEquals(-1L, coordinator.fetchOffset(bPartition));

            // owner b commits successfully
            CommitResult ok = b.commit(bPartition, 10);
            assertTrue(ok.accepted());
            assertEquals(10L, coordinator.fetchOffset(bPartition));

            // stale generation: simulate an old-generation token directly
            CommitResult stale = coordinator.commit("b", b.incarnationId(),
                    gen - 1, bPartition, 11);
            assertFalse(stale.accepted());
            assertEquals("STALE_GENERATION", stale.rejectReason());
            assertEquals(10L, coordinator.fetchOffset(bPartition), "state must not move");
        } finally {
            clients.values().forEach(ConsumerClient::close);
        }
    }

    @Test
    void crashRestartResumesFromConfirmedOffset() throws Exception {
        // initial process consumes and commits
        try (ConsumerClient c1 = new ConsumerClient("127.0.0.1", port, "worker")) {
            c1.join(3_000);
            c1.startHeartbeat(200);
            for (long o = 0; o < 20; o++) {
                CommitResult r = c1.commit(0, o + 1);
                assertTrue(r.accepted(), "commit " + (o + 1) + " should be accepted");
            }
            assertEquals(20L, coordinator.fetchOffset(0));
            // simulate crash: no graceful leave, just close
        }

        // restart as a new process/incarnation with the SAME member id
        try (ConsumerClient c2 = new ConsumerClient("127.0.0.1", port, "worker")) {
            c2.join(3_000);
            long resume = c2.fetchOffset(0);
            assertEquals(20L, resume, "must resume at the last confirmed offset");
            CommitResult r = c2.commit(0, 21);
            assertTrue(r.accepted());
            assertEquals(21L, coordinator.fetchOffset(0));
            // going backwards is rejected
            CommitResult back = c2.commit(0, 5);
            assertFalse(back.accepted());
            assertEquals("REGRESSION", back.rejectReason());
            assertEquals(21L, coordinator.fetchOffset(0));
        }
    }

    @Test
    void interleavedCommitsDuringRebalanceAreNeverLostOrMisassigned() throws Exception {
        Map<String, ConsumerClient> clients = new HashMap<>();
        Set<String> base = new HashSet<>(Set.of("a", "b"));
        for (String m : base) {
            clients.put(m, new ConsumerClient("127.0.0.1", port, m));
        }
        java.util.concurrent.atomic.AtomicBoolean churnDone =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        // snapshot of offsets each worker believes are durable; it only ever
        // advances by reading the coordinator, so a lost update would show as
        // a durable value behind the accepted count.
        try {
            stabilize(base, clients);

            ExecutorService pool = Executors.newFixedThreadPool(2);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> fs = new java.util.ArrayList<>();
            for (String m : List.of("a", "b")) {
                final String id = m;
                fs.add(pool.submit(() -> {
                    try {
                        start.await();
                        ConsumerClient c = clients.get(id);
                        while (!churnDone.get()) {
                            if (c.needsRejoin() || c.partitions().isEmpty()) {
                                Thread.sleep(20);
                                continue;
                            }
                            // commit the next offset for each owned partition,
                            // basing it on the last confirmed durable value
                            for (int p : c.partitions()) {
                                long next = coordinator.fetchOffset(p) + 1;
                                CommitResult r = c.commit(p, next);
                                if (r.accepted()) {
                                    accepted.incrementAndGet();
                                    // Ownership fence at commit time: the coordinator
                                    // accepted only because this member was the unique
                                    // owner. A newer rebalance may already have revoked
                                    // it (empty), but it can never be a different owner.
                                    var owners = coordinator.ownersOf(p);
                                    assertTrue(owners.isEmpty() || owners.equals(List.of(id)),
                                            "partition " + p + " unexpectedly owned by "
                                                    + owners + " right after " + id + " committed");
                                    // Monotonic durable value: never lost/regressed.
                                    assertTrue(coordinator.fetchOffset(p) >= next,
                                            "durable offset regressed for partition " + p);
                                } else {
                                    rejected.incrementAndGet();
                                }
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            start.countDown();

            // Drive membership churn from the single test thread while workers commit.
            try (ConsumerClient c3 = new ConsumerClient("127.0.0.1", port, "c")) {
                for (int i = 0; i < 4; i++) {
                    Map<String, ConsumerClient> withC = addClient(clients, "c", c3);
                    stabilize(new HashSet<>(Set.of("a", "b", "c")), withC);
                    // refresh the shared map so c is known to final stabilization
                    clients.put("c", c3);
                    c3.leave();
                    clients.remove("c");
                    stabilize(base, clients);
                }
            }
            churnDone.set(true);
            for (Future<?> f : fs) {
                f.get(30, TimeUnit.SECONDS);
            }
            pool.shutdown();

            stabilize(base, clients);
            assertFalse(audit.hasSimultaneousOverlap());
            assertTrue(accepted.get() > 0, "some commits must succeed");
            // After convergence every partition has a single owner and covers all.
            assertEquals(Set.of(0, 1, 2, 3, 4, 5, 6, 7),
                    coordinator.snapshot().assignments().values().stream()
                            .flatMap(List::stream).collect(java.util.stream.Collectors.toSet()));
        } finally {
            churnDone.set(true);
            clients.values().forEach(ConsumerClient::close);
        }
    }

    private Map<String, ConsumerClient> addClient(Map<String, ConsumerClient> base,
                                                  String id, ConsumerClient c) {
        Map<String, ConsumerClient> copy = new HashMap<>(base);
        copy.put(id, c);
        return copy;
    }
}
