package com.github.lechandonga.cgcoordinator.group;

import com.github.lechandonga.cgcoordinator.assign.RangeAssignor;
import com.github.lechandonga.cgcoordinator.audit.OwnershipAuditLog;
import com.github.lechandonga.cgcoordinator.config.CoordinatorConfig;
import com.github.lechandonga.cgcoordinator.model.Assignment;
import com.github.lechandonga.cgcoordinator.model.CommitResult;
import com.github.lechandonga.cgcoordinator.offset.FileOffsetStore;
import com.github.lechandonga.cgcoordinator.reject.CoordinatorException;
import com.github.lechandonga.cgcoordinator.reject.RejectReason;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GroupCoordinatorTest {

    @TempDir
    Path dir;

    private GroupCoordinator newCoordinator(int partitions, long sessionMs, long joinMs) {
        CoordinatorConfig cfg = new CoordinatorConfig(partitions, sessionMs,
                Math.max(1, sessionMs / 3), joinMs, dir.toString());
        return new GroupCoordinator(cfg, new RangeAssignor(),
                new FileOffsetStore(dir.toString(), partitions),
                new OwnershipAuditLog());
    }

    private GroupCoordinator newCoordinator(int partitions) {
        return newCoordinator(partitions, 5_000, 2_000);
    }

    /**
     * Join all members concurrently and keep re-joining until the group is
     * STABLE with every member in one single generation. This mirrors what
     * well-behaved clients do and removes scheduling-jitter sensitivity.
     */
    private long joinAllStable(GroupCoordinator c, List<String> members) throws Exception {
        try (ExecutorService pool = Executors.newFixedThreadPool(members.size())) {
            for (int round = 0; round < 6; round++) {
                CountDownLatch start = new CountDownLatch(1);
                List<Future<Assignment>> fs = new ArrayList<>();
                for (String m : members) {
                    final String id = m;
                    fs.add(pool.submit(() -> {
                        start.await();
                        return c.joinGroup(id, id + "-1", 3_000);
                    }));
                }
                start.countDown();
                for (Future<Assignment> f : fs) {
                    f.get(6, TimeUnit.SECONDS);
                }
                if (c.state() == GroupState.STABLE
                        && new java.util.HashSet<>(c.snapshot().members())
                                .equals(new java.util.HashSet<>(members))) {
                    return c.generation();
                }
            }
        }
        throw new AssertionError("group did not stabilize with members " + members);
    }

    @Test
    void singleMemberJoinCoversAllPartitionsAndIsStable() {
        GroupCoordinator c = newCoordinator(4);
        Assignment a = c.joinGroup("a", "a-1", 2_000);
        assertEquals(1, a.generation());
        assertEquals(List.of(0, 1, 2, 3), a.partitions());
        assertEquals(GroupState.STABLE, c.state());
        assertTrue(c.assignmentCoversAndDisjoint());
    }

    @Test
    void secondMemberTriggersRebalanceWithFullCoverAndDisjointOwnership() throws Exception {
        GroupCoordinator c = newCoordinator(6);
        c.joinGroup("a", "a-1", 2_000);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            pool.shutdown();
        } finally {
            pool.shutdownNow();
        }
        long gen = joinAllStable(c, List.of("a", "b"));
        assertTrue(gen > 1);
        List<Integer> pa = c.snapshot().assignments().get("a");
        List<Integer> pb = c.snapshot().assignments().get("b");
        Set<Integer> all = new HashSet<>(pa);
        all.addAll(pb);
        assertEquals(Set.of(0, 1, 2, 3, 4, 5), all);
        assertEquals(6, all.size());
        assertTrue(c.assignmentCoversAndDisjoint());
        for (int p0 = 0; p0 < 6; p0++) {
            assertEquals(1, c.ownersOf(p0).size());
        }
    }

    @Test
    void staleGenerationHeartbeatAndCommitAreRejected() throws Exception {
        GroupCoordinator c = newCoordinator(2);
        Assignment a = c.joinGroup("a", "a-1", 2_000);
        // Simulate generation advance via a second member joining concurrently.
        ExecutorService pool = Executors.newFixedThreadPool(1);
        try {
            Future<?> f = pool.submit(() -> c.joinGroup("b", "b-1", 3_000));
            // a must re-join too; first do rejected calls on old generation
            CoordinatorException hb = assertThrows(CoordinatorException.class,
                    () -> c.heartbeat("a", "a-1", a.generation()));
            // During rebalance heartbeat is REBALANCING; after it completes it
            // would be STALE_GENERATION. Both are distinguishable rejections.
            assertTrue(hb.reason() == RejectReason.REBALANCING
                    || hb.reason() == RejectReason.STALE_GENERATION);
            // keep a in the live set so the rebalance completes
            c.joinGroup("a", "a-1", 3_000);
            f.get(5, TimeUnit.SECONDS);
            CommitResult r = c.commit("a", "a-1", a.generation(), 0, 1);
            assertFalse(r.accepted());
            assertEquals(RejectReason.STALE_GENERATION.name(), r.rejectReason());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void zombieIncarnationIsFenced() throws Exception {
        GroupCoordinator c = newCoordinator(2);
        c.joinGroup("a", "a-1", 2_000);
        // restart member a with a new incarnation id
        ExecutorService pool = Executors.newFixedThreadPool(1);
        try {
            Future<?> f = pool.submit(() -> c.joinGroup("a", "a-2", 3_000));
            c.joinGroup("a", "a-2", 3_000);
            f.get(5, TimeUnit.SECONDS);
            CoordinatorException ex = assertThrows(CoordinatorException.class,
                    () -> c.heartbeat("a", "a-1", c.generation()));
            assertEquals(RejectReason.FENCED_INCARNATION, ex.reason());
            CommitResult r = c.commit("a", "a-1", c.generation(), 0, 1);
            assertFalse(r.accepted());
            assertEquals(RejectReason.FENCED_INCARNATION.name(), r.rejectReason());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void commitByNonOwnerIsRejectedWithoutChangingState() throws Exception {
        GroupCoordinator c = newCoordinator(4);
        long gen = joinAllStable(c, List.of("a", "b"));
        List<Integer> pb = c.snapshot().assignments().get("b");
        for (int foreign : pb) {
            CommitResult r = c.commit("a", "a-1", gen, foreign, 1);
            assertFalse(r.accepted());
            assertEquals(RejectReason.NOT_OWNER.name(), r.rejectReason());
            assertEquals(-1L, c.fetchOffset(foreign));
        }
    }

    @Test
    void voluntaryLeaveTriggersRebalanceAndReassignsAllPartitions() throws Exception {
        GroupCoordinator c = newCoordinator(4);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Assignment>> fs = new ArrayList<>();
            for (String m : List.of("a", "b")) {
                final String id = m;
                fs.add(pool.submit(() -> {
                    start.await();
                    return c.joinGroup(id, id + "-1", 3_000);
                }));
            }
            start.countDown();
            fs.forEach(f -> {
                try { f.get(5, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
            });
            long genBefore = c.generation();
            Future<?> aRejoin = pool.submit(() -> c.joinGroup("a", "a-1", 3_000));
            c.leave("b", "b-1", genBefore);
            aRejoin.get(5, TimeUnit.SECONDS);
            assertEquals(Set.of("a"), Set.copyOf(c.snapshot().members()));
            assertEquals(List.of(0, 1, 2, 3), c.snapshot().assignments().get("a"));
            assertTrue(c.assignmentCoversAndDisjoint());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void sessionExpiryRemovesDeadMemberAndRebalances() throws Exception {
        GroupCoordinator c = newCoordinator(4, 200, 500);
        c.joinGroup("a", "a-1", 1_000);
        c.joinGroup("b", "b-1", 1_000);
        long oldGen = c.generation();
        // b stops heartbeating; expire after session timeout.
        Thread.sleep(260);
        assertTrue(c.expireSessions(System.nanoTime()));
        // a re-joins the triggered rebalance
        c.joinGroup("a", "a-1", 1_000);
        assertTrue(c.generation() > oldGen);
        assertEquals(Set.of("a"), Set.copyOf(c.snapshot().members()));
        assertTrue(c.assignmentCoversAndDisjoint());
    }

    @Test
    void rejectedCommitDoesNotChangeEstablishedOwnership() {
        GroupCoordinator c = newCoordinator(2);
        Assignment a = c.joinGroup("a", "a-1", 1_000);
        CommitResult ok = c.commit("a", "a-1", a.generation(), 0, 5);
        assertTrue(ok.accepted());
        CommitResult bad = c.commit("a", "a-1", a.generation(), 0, 4);
        assertFalse(bad.accepted());
        assertEquals(RejectReason.REGRESSION.name(), bad.rejectReason());
        assertEquals(5L, c.fetchOffset(0));
        assertEquals(List.of("a"), c.ownersOf(0));
    }
}
