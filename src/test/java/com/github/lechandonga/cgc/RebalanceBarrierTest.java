package com.github.lechandonga.cgc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BARRIER mode: membership changes open a confirmation barrier with a real
 * time bound. Requests observe a distinguishable in-progress result; the
 * barrier settles deterministically at the configured deadline.
 */
class RebalanceBarrierTest {

    @TempDir
    Path dir;

    private MutableClock clock;
    private GroupCoordinator coordinator;

    private GroupCoordinator newCoordinator(int partitions, long sessionTimeoutMs,
                                            long rebalanceTimeoutMs) {
        clock = new MutableClock(0);
        CoordinatorConfig config = new CoordinatorConfig(
                "topic", partitions, 50, sessionTimeoutMs, rebalanceTimeoutMs,
                dir.resolve("offsets-" + System.nanoTime() + ".log"),
                RebalanceMode.BARRIER);
        coordinator = new GroupCoordinator(config, clock);
        return coordinator;
    }

    @Test
    void singleMemberJoinSettlesImmediately() {
        GroupCoordinator c = newCoordinator(4, 400, 1000);
        HeartbeatResponse r = c.join("m1");
        assertFalse(r.rebalanceInProgress());
        assertEquals(GroupState.STABLE, c.state());
        assertEquals(4, r.assignment().size());
        c.close();
    }

    @Test
    void requestsDuringBarrierGetDistinguishableInProgress() throws Exception {
        GroupCoordinator c = newCoordinator(4, 400, 1000);
        HeartbeatResponse m1 = c.join("m1");
        c.join("m2"); // opens the barrier; m1 has not confirmed yet
        assertEquals(GroupState.REBALANCING, c.state());

        CoordinatorException hb = assertThrows(CoordinatorException.class,
                () -> c.heartbeat("m1", m1.memberEpoch(), m1.generation()));
        assertEquals(RejectionReason.REBALANCE_IN_PROGRESS, hb.reason(),
                "in-progress must be distinguishable from terminal failures");

        CoordinatorException commit = assertThrows(CoordinatorException.class,
                () -> c.commit("m1", m1.memberEpoch(), m1.generation(),
                        new Partition("topic", 0), 1));
        assertEquals(RejectionReason.REBALANCE_IN_PROGRESS, commit.reason());

        // m1 confirms by re-joining; the barrier settles.
        HeartbeatResponse confirmed = c.join("m1");
        assertFalse(confirmed.rebalanceInProgress());
        assertEquals(GroupState.STABLE, c.state());
        assertEquals(2, c.assignmentOf("m1").size());
        assertEquals(2, c.assignmentOf("m2").size());
        assertTrue(c.verifyNoOwnershipOverlap());
        c.close();
    }

    @Test
    void unconfirmedMemberIsEvictedAtDeadline() throws Exception {
        GroupCoordinator c = newCoordinator(4, 400, 1000);
        HeartbeatResponse m1 = c.join("m1");
        HeartbeatResponse m2 = c.join("m2");
        assertEquals(GroupState.REBALANCING, c.state());
        int generationAtBarrier = c.generation();

        // Both stay session-alive (heartbeats refresh liveness even while
        // reporting in-progress) but m1 never confirms the barrier.
        for (int t = 300; t <= 900; t += 300) {
            clock.advance(300);
            assertThrows(CoordinatorException.class,
                    () -> c.heartbeat("m1", m1.memberEpoch(), m1.generation()));
            assertThrows(CoordinatorException.class,
                    () -> c.heartbeat("m2", m2.memberEpoch(), m2.generation()));
        }
        clock.advance(101); // now = 1001 > deadline 1000
        c.checkExpirations();

        // Deterministic conclusion: m1 evicted, m2 owns everything.
        assertEquals(GroupState.STABLE, c.state());
        assertEquals(Set.of("m2"), c.members());
        assertEquals(4, c.assignmentOf("m2").size());
        assertEquals(generationAtBarrier, c.generation(),
                "force-completion settles the pending generation, not a new one");
        assertTrue(c.verifyNoOwnershipOverlap());
        c.close();
    }

    @Test
    void rebalanceTimeoutParameterTakesEffect() throws Exception {
        GroupCoordinator shortTimeout = newCoordinator(4, 400, 500);
        HeartbeatResponse a1 = shortTimeout.join("m1");
        HeartbeatResponse a2 = shortTimeout.join("m2");
        MutableClock shortClock = clock;

        GroupCoordinator longTimeout = newCoordinator(4, 400, 5000);
        HeartbeatResponse b1 = longTimeout.join("m1");
        HeartbeatResponse b2 = longTimeout.join("m2");
        MutableClock longClock = clock;

        // Same liveness pattern in both groups (heartbeats report in-progress
        // but still refresh the session).
        shortClock.advance(300);
        longClock.advance(300);
        assertThrows(CoordinatorException.class,
                () -> shortTimeout.heartbeat("m1", a1.memberEpoch(), a1.generation()));
        assertThrows(CoordinatorException.class,
                () -> shortTimeout.heartbeat("m2", a2.memberEpoch(), a2.generation()));
        assertThrows(CoordinatorException.class,
                () -> longTimeout.heartbeat("m1", b1.memberEpoch(), b1.generation()));
        assertThrows(CoordinatorException.class,
                () -> longTimeout.heartbeat("m2", b2.memberEpoch(), b2.generation()));

        shortClock.advance(201); // t=501 > 500
        longClock.advance(201);  // t=501 < 5000
        shortTimeout.checkExpirations();
        longTimeout.checkExpirations();

        assertEquals(GroupState.STABLE, shortTimeout.state(),
                "short timeout must have settled the barrier deterministically");
        assertEquals(Set.of("m2"), shortTimeout.members());

        assertEquals(GroupState.REBALANCING, longTimeout.state(),
                "long timeout must still be waiting for confirmations");
        assertEquals(Set.of("m1", "m2"), longTimeout.members());
        CoordinatorException e = assertThrows(CoordinatorException.class,
                () -> longTimeout.heartbeat("m1", b1.memberEpoch(), b1.generation()));
        assertEquals(RejectionReason.REBALANCE_IN_PROGRESS, e.reason());

        shortTimeout.close();
        longTimeout.close();
    }

    @Test
    void joinAndAwaitReturnsDefiniteConclusion() throws Exception {
        GroupCoordinator c = newCoordinator(4, 400, 1000);
        c.join("m1");

        AtomicReference<HeartbeatResponse> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                result.set(c.joinAndAwait("m2"));
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        waiter.start();

        // The join opens the barrier and blocks; m1 never confirms.
        awaitTrue(() -> c.state() == GroupState.REBALANCING);
        // Advance in steps so the waiting join can refresh its session liveness
        // against the moving clock (as it does in real time).
        for (int i = 0; i < 4; i++) {
            clock.advance(300);
            Thread.sleep(150);
        }
        c.checkExpirations(); // deterministic conclusion: m1 evicted
        waiter.join(10_000);

        assertEquals(null, failure.get(), "waiter must not fail");
        HeartbeatResponse r = result.get();
        assertFalse(r.rebalanceInProgress(), "waiter must get a final, not in-progress, result");
        assertEquals(4, r.assignment().size(), "survivor takes over all partitions");
        assertEquals(Set.of("m2"), c.members());
        assertEquals(GroupState.STABLE, c.state());
        c.close();
    }

    @Test
    void joinAndAwaitReturnsImmediatelyWhenAllConfirm() throws Exception {
        GroupCoordinator c = newCoordinator(4, 400, 60_000);
        c.join("m1");
        c.join("m2"); // barrier open, m1 unconfirmed
        HeartbeatResponse r = c.joinAndAwait("m1"); // confirms -> settles now
        assertFalse(r.rebalanceInProgress());
        assertEquals(2, r.assignment().size());
        assertEquals(GroupState.STABLE, c.state());
        assertEquals(Set.of("m1", "m2"), c.members());
        c.close();
    }

    private static void awaitTrue(Check condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!condition.check()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("condition not met within 10s");
            }
            Thread.sleep(5);
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean check();
    }
}
