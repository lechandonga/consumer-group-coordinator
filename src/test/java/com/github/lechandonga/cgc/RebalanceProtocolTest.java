package com.github.lechandonga.cgc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Deterministic tests for the multi-process protocol exercised in-process
 * with the injectable clock: two-phase join, the rebalance time bound and
 * IN_PROGRESS observability, timeout-driven deterministic close, and
 * instance-identity fencing after takeover.
 *
 * <p>Protocol semantics: phase-1 {@code joinGroup} is a member's signal that
 * it participates in the open generation; the window closes when every member
 * that existed when it opened has called joinGroup for it, or at the bound.
 */
class RebalanceProtocolTest {

    @TempDir
    Path dir;

    private MutableClock clock;
    private GroupCoordinator coordinator;

    private GroupCoordinator newCoordinator(int partitions, long rebalanceTimeoutMs) {
        return newCoordinator(partitions, 200, rebalanceTimeoutMs);
    }

    private GroupCoordinator newCoordinator(int partitions, long sessionTimeoutMs,
                                            long rebalanceTimeoutMs) {
        clock = new MutableClock(0);
        CoordinatorConfig config = new CoordinatorConfig(
                "topic", partitions, 20, sessionTimeoutMs, rebalanceTimeoutMs,
                dir.resolve("offsets-" + System.nanoTime() + ".log"));
        coordinator = new GroupCoordinator(config, clock);
        return coordinator;
    }

    private static void assertCompleteAndDisjoint(GroupCoordinator c, int partitionCount) {
        Map<Partition, String> ownership = c.ownershipSnapshot();
        assertEquals(partitionCount, ownership.size());
        Map<String, Set<Partition>> byMember = new HashMap<>();
        ownership.forEach((p, m) -> byMember.computeIfAbsent(m, k -> new HashSet<>()).add(p));
        int total = byMember.values().stream().mapToInt(Set::size).sum();
        assertEquals(partitionCount, total, "assignments must not overlap");
        assertEquals(new HashSet<>(c.members()), byMember.keySet());
    }

    /** First-ever member: join then sync closes its single-member window. */
    private JoinResult bootstrap(String memberId) throws CoordinatorException {
        JoinResult joined = coordinator.joinGroup(memberId, null);
        assertEquals(JoinStatus.IN_PROGRESS, joined.status());
        return coordinator.syncGroup(memberId, joined.instanceToken(), joined.generation());
    }

    private final Map<String, String> ownerByToken = new HashMap<>();

    private String tokenOwner(String token) {
        String owner = ownerByToken.get(token);
        if (owner == null) {
            throw new IllegalStateException("unknown test token " + token);
        }
        return owner;
    }

    private JoinResult remember(String id, JoinResult r) {
        ownerByToken.put(r.instanceToken(), id);
        return r;
    }

    @Test
    void singleMemberInProgressThenStable() throws Exception {
        GroupCoordinator c = newCoordinator(3, 500);
        JoinResult joined = c.joinGroup("a", null);
        assertEquals(JoinStatus.IN_PROGRESS, joined.status());
        assertEquals(GroupState.REBALANCING, c.state());
        assertTrue(joined.instanceToken().startsWith("inst-"));
        JoinResult synced = c.syncGroup("a", joined.instanceToken(), joined.generation());
        assertEquals(JoinStatus.STABLE, synced.status());
        assertEquals(3, synced.assignment().size());
        assertFalse(synced.completedByTimeout());
        assertCompleteAndDisjoint(c, 3);
        c.close();
    }

    @Test
    void twoPhaseJoinDistributesPartitionsAndAdvancesGeneration() throws Exception {
        GroupCoordinator c = newCoordinator(4, 500);
        JoinResult a = bootstrap("a");
        assertEquals(4, a.assignment().size());

        // b arrives: window opens and is observable as in-progress.
        JoinResult bJoin = c.joinGroup("b", null);
        assertEquals(JoinStatus.IN_PROGRESS, bJoin.status());
        int openGen = bJoin.generation();
        assertTrue(openGen > a.generation());
        // Neither member has finished phase 2 yet (b issued phase 1, a is the
        // existing member the window is waiting for).
        assertEquals(Set.of("a", "b"), c.pendingAwaiting());

        // 'a' still on the old generation sees the open window as in-progress,
        // which is distinguishable from a final rejection.
        CoordinatorException hb = assertThrows(CoordinatorException.class,
                () -> c.heartbeat("a", a.instanceToken(), a.generation()));
        assertEquals(RejectionReason.REBALANCE_IN_PROGRESS, hb.reason());

        // b finishes phase 2 but a (the existing member) has not: still open.
        JoinResult bWaiting = c.syncGroup("b", bJoin.instanceToken(), openGen);
        assertEquals(JoinStatus.IN_PROGRESS, bWaiting.status());
        assertEquals(Set.of("a"), c.pendingAwaiting());

        // a performs phase 1 AND phase 2 for the new generation -> window closes.
        JoinResult aRejoin = c.joinGroup("a", a.instanceToken());
        assertEquals(JoinStatus.IN_PROGRESS, aRejoin.status());
        JoinResult a2 = c.syncGroup("a", a.instanceToken(), openGen);
        JoinResult b = c.syncGroup("b", bJoin.instanceToken(), openGen);
        assertEquals(JoinStatus.STABLE, a2.status());
        assertEquals(JoinStatus.STABLE, b.status());
        assertEquals(2, a2.assignment().size());
        assertEquals(2, b.assignment().size());
        assertCompleteAndDisjoint(c, 4);
        assertTrue(c.verifyNoOwnershipOverlap());
        c.close();
    }

    @Test
    void commitsWhileRebalancingReturnInProgressNotOrdinaryFailure() throws Exception {
        GroupCoordinator c = newCoordinator(2, 500);
        JoinResult a = bootstrap("a");
        Partition p0 = new Partition("topic", 0);
        c.commit("a", a.instanceToken(), a.generation(), p0, 7);

        // b arrives; the window stays open (a has not rejoined yet).
        JoinResult bJoin = c.joinGroup("b", null);

        // In-progress outcome: distinct reason, no state mutation.
        CoordinatorException commit = assertThrows(CoordinatorException.class,
                () -> c.commit("a", a.instanceToken(), a.generation(), p0, 8));
        assertEquals(RejectionReason.REBALANCE_IN_PROGRESS, commit.reason());
        assertEquals(7, c.fetchCommitted(p0), "in-progress commit must not mutate state");

        // a performs phase 1 and both finish phase 2 -> window closes; now an
        // old-generation commit is a *final* failure (distinct reason), and b
        // can progress on its partition.
        c.joinGroup("a", a.instanceToken());
        // b finishes phase 2 first but the window is still open (a not yet).
        assertEquals(JoinStatus.IN_PROGRESS,
                c.syncGroup("b", bJoin.instanceToken(), bJoin.generation()).status());
        // a is the last to finish phase 2 -> the window closes.
        JoinResult aStable = c.syncGroup("a", a.instanceToken(), bJoin.generation());
        JoinResult b = c.syncGroup("b", bJoin.instanceToken(), bJoin.generation());
        assertEquals(JoinStatus.STABLE, b.status());
        assertEquals(JoinStatus.STABLE, aStable.status());
        CoordinatorException stale = assertThrows(CoordinatorException.class,
                () -> c.commit("a", a.instanceToken(), a.generation(), p0, 8));
        assertEquals(RejectionReason.STALE_GENERATION, stale.reason());
        assertEquals(7, c.fetchCommitted(p0));
        Partition ownedByB = b.assignment().iterator().next();
        c.commit("b", b.instanceToken(), b.generation(), ownedByB, 0);
        c.close();
    }

    @Test
    void rebalanceClosesAtBoundAndEvictsNonRejoiningMember() throws Exception {
        GroupCoordinator c = newCoordinator(4, 300);
        JoinResult a = remember("a", bootstrap("a"));
        // Bring b up stably: a must rejoin the generation b opened.
        JoinResult bJoin = c.joinGroup("b", null);
        c.joinGroup("a", a.instanceToken());
        JoinResult a2 = c.syncGroup("a", a.instanceToken(), bJoin.generation());
        JoinResult b = remember("b", c.syncGroup("b", bJoin.instanceToken(), bJoin.generation()));
        assertEquals(2, b.assignment().size());
        assertEquals(a2.generation(), b.generation());

        // c arrives (phase 1). Nobody has finished phase 2 of the new window.
        JoinResult xJoin = c.joinGroup("c", null);
        assertEquals(JoinStatus.IN_PROGRESS, xJoin.status());
        assertEquals(Set.of("a", "b", "c"), c.pendingAwaiting());
        long deadline = c.rebalanceDeadlineMs();
        assertEquals(300, deadline - clock.getAsLong());

        // c finishes phase 2 but a and b have not; before the bound the window
        // is still open and nothing is finalized.
        clock.advance(299);
        JoinResult waiting = c.syncGroup("c", xJoin.instanceToken(), xJoin.generation());
        assertEquals(JoinStatus.IN_PROGRESS, waiting.status());
        assertEquals(GroupState.REBALANCING, c.state());

        // Bound elapsed: deterministic, observable close. a and b (silent, no
        // sync and no renewal) are evicted; c owns everything.
        clock.advance(2);
        JoinResult closed = c.syncGroup("c", xJoin.instanceToken(), xJoin.generation());
        assertEquals(JoinStatus.STABLE, closed.status());
        assertTrue(closed.completedByTimeout(),
                "close at the time bound must be flagged as timeout completion");
        assertTrue(c.lastCompletedByTimeout());
        assertEquals(Set.of("c"), c.members());
        assertEquals(4, closed.assignment().size());
        assertCompleteAndDisjoint(c, 4);
        assertTrue(c.verifyNoOwnershipOverlap());
        c.close();
    }

    @Test
    void timeoutValueChangesBehavior() throws Exception {
        // With a 100ms bound the window closes before 500ms; with 1000ms it is
        // still observably open at 500ms. The parameter must take effect.
        for (long bound : new long[]{100, 1000}) {
            GroupCoordinator c = newCoordinator(2, 50, bound);
            JoinResult a = bootstrap("a");
            JoinResult bJoin = c.joinGroup("b", null);
            clock.advance(500);
            JoinResult r = c.syncGroup("b", bJoin.instanceToken(), bJoin.generation());
            if (bound <= 500) {
                assertEquals(JoinStatus.STABLE, r.status(), "bound=" + bound);
                assertTrue(r.completedByTimeout());
            } else {
                assertEquals(JoinStatus.IN_PROGRESS, r.status(), "bound=" + bound);
            }
            c.close();
        }
    }

    @Test
    void pendingWindowClosesAtBoundEvenWithoutSyncCalls() throws Exception {
        GroupCoordinator c = newCoordinator(4, 200);
        JoinResult a = remember("a", bootstrap("a"));
        JoinResult bJoin = c.joinGroup("b", null);
        c.joinGroup("a", a.instanceToken());
        c.syncGroup("a", a.instanceToken(), bJoin.generation());
        JoinResult b = remember("b", c.syncGroup("b", bJoin.instanceToken(), bJoin.generation()));
        // c opens gen3 and finishes phase 2; a and b never participate nor renew.
        JoinResult cj = c.joinGroup("c", null);
        c.syncGroup("c", cj.instanceToken(), cj.generation());
        clock.advance(250); // beyond the bound
        // The next ordinary request lazily closes the timed-out window; a and
        // b (silent) were evicted and their old tokens are fenced, so the stale
        // a process is told FENCED_INSTANCE (not silently unknown).
        CoordinatorException e = assertThrows(CoordinatorException.class,
                () -> c.heartbeat("a", a.instanceToken(), 1));
        assertEquals(RejectionReason.FENCED_INSTANCE, e.reason());
        // A token that was never valid is still UNKNOWN_MEMBER, not fenced.
        CoordinatorException bogus = assertThrows(CoordinatorException.class,
                () -> c.heartbeat("a", "inst-never-existed", 1));
        assertEquals(RejectionReason.UNKNOWN_MEMBER, bogus.reason());
        assertEquals(GroupState.STABLE, c.state());
        assertEquals(Set.of("c"), c.members());
        assertCompleteAndDisjoint(c, 4);
        c.close();
    }

    @Test
    void newInstanceTakesOverAndOldInstanceIsFencedOnHeartbeatAndCommit() throws Exception {
        GroupCoordinator c = newCoordinator(2, 300);
        JoinResult old = bootstrap("a");
        Partition p0 = new Partition("topic", 0);
        c.commit("a", old.instanceToken(), old.generation(), p0, 5);

        // A new process restarts with the same member id but a blank token.
        JoinResult neoJoin = c.joinGroup("a", null);
        assertNotEquals(old.instanceToken(), neoJoin.instanceToken());
        // Only 'a' is expected, so its new instance closes the window itself.
        JoinResult neo = c.syncGroup("a", neoJoin.instanceToken(), neoJoin.generation());
        assertEquals(JoinStatus.STABLE, neo.status());
        assertTrue(neo.generation() > old.generation());

        // The old instance, still alive elsewhere, is rejected on every
        // renewing/mutating request, no matter which generation it claims.
        CoordinatorException hb = assertThrows(CoordinatorException.class,
                () -> c.heartbeat("a", old.instanceToken(), neo.generation()));
        assertEquals(RejectionReason.FENCED_INSTANCE, hb.reason());
        CoordinatorException commitOldGen = assertThrows(CoordinatorException.class,
                () -> c.commit("a", old.instanceToken(), old.generation(), p0, 6));
        assertEquals(RejectionReason.FENCED_INSTANCE, commitOldGen.reason());
        CoordinatorException commitNewGen = assertThrows(CoordinatorException.class,
                () -> c.commit("a", old.instanceToken(), neo.generation(), p0, 6));
        assertEquals(RejectionReason.FENCED_INSTANCE, commitNewGen.reason());

        // Fenced attempts leave the committed position untouched...
        assertEquals(5, c.fetchCommitted(p0));
        // ...and the new instance cleanly owns the identity and progresses.
        c.commit("a", neo.instanceToken(), neo.generation(), p0, 6);
        assertEquals(6, c.fetchCommitted(p0));
        assertEquals(2, neo.assignment().size());
        c.close();
    }

    @Test
    void oldInstanceKeepsBeingFencedAfterMembershipSettles() throws Exception {
        GroupCoordinator c = newCoordinator(4, 300);
        JoinResult oldA = remember("a", bootstrap("a"));
        // Bring b up stably.
        JoinResult bJoin = c.joinGroup("b", null);
        c.joinGroup("a", oldA.instanceToken());
        JoinResult b = remember("b", c.syncGroup("b", bJoin.instanceToken(), bJoin.generation()));

        // a restarts as a new instance; b rejoins so the window closes.
        JoinResult neoAJoin = c.joinGroup("a", null);
        c.joinGroup("b", b.instanceToken());
        JoinResult neoA = c.syncGroup("a", neoAJoin.instanceToken(), neoAJoin.generation());
        assertEquals(JoinStatus.STABLE, neoA.status());

        // Old a keeps sending heartbeats: fenced every time, never re-admitted,
        // and it cannot block the settled group.
        for (int i = 0; i < 5; i++) {
            CoordinatorException e = assertThrows(CoordinatorException.class,
                    () -> c.heartbeat("a", oldA.instanceToken(), neoA.generation()));
            assertEquals(RejectionReason.FENCED_INSTANCE, e.reason());
        }
        assertEquals(GroupState.STABLE, c.state());
        assertCompleteAndDisjoint(c, 4);
        c.close();
    }

    @Test
    void syncWithUnknownGenerationIsRejectedNotInProgress() throws Exception {
        GroupCoordinator c = newCoordinator(2, 500);
        JoinResult a = bootstrap("a");
        JoinResult bJoin = c.joinGroup("b", null);
        CoordinatorException stale = assertThrows(CoordinatorException.class,
                () -> c.syncGroup("b", bJoin.instanceToken(), a.generation()));
        assertEquals(RejectionReason.STALE_GENERATION, stale.reason());
        CoordinatorException future = assertThrows(CoordinatorException.class,
                () -> c.syncGroup("b", bJoin.instanceToken(), bJoin.generation() + 9));
        assertEquals(RejectionReason.ILLEGAL_GENERATION, future.reason());
        c.close();
    }

    @Test
    void expiredMemberIsEvictedMidWindowAndWindowCloses() throws Exception {
        GroupCoordinator c = newCoordinator(4, 300, 800);
        JoinResult a = remember("a", bootstrap("a"));
        JoinResult bJoin = c.joinGroup("b", null);
        c.joinGroup("a", a.instanceToken());
        JoinResult b = remember("b", c.syncGroup("b", bJoin.instanceToken(), bJoin.generation()));

        // c opens a generation. b goes silent; a and c keep their sessions alive.
        JoinResult xJoin = c.joinGroup("c", null);
        clock.advance(200);
        c.syncGroup("a", a.instanceToken(), xJoin.generation()); // a alive & participates
        c.syncGroup("c", xJoin.instanceToken(), xJoin.generation()); // c alive
        clock.advance(200); // b idle 400ms > sessionTimeout(300); a/c idle 200ms
        c.checkExpirations();
        assertEquals(Set.of("a", "c"), c.members());
        // With b evicted, the open generation closes: complete coverage remains.
        assertEquals(GroupState.STABLE, c.state());
        assertCompleteAndDisjoint(c, 4);
        assertTrue(c.verifyNoOwnershipOverlap());
        c.close();
    }
}
