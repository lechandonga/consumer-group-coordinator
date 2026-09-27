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
 * Deterministic tests (injectable clock) for interleaved membership changes:
 * several members join / change at almost the same time, so the rebalance
 * window one member is confirming gets superseded by a newer change before
 * that member finishes phase 2.
 *
 * <p>Locked-in semantics:
 * <ul>
 *   <li>confirming a superseded-but-joined generation yields a retryable
 *       IN_PROGRESS for the generation open now — never a final failure;</li>
 *   <li>a live, current member always ends up with its share of the stable
 *       generation; the union of assignments covers every partition exactly
 *       once;</li>
 *   <li>the configured time bound still closes the window deterministically
 *       (no infinite IN_PROGRESS);</li>
 *   <li>superseded / evicted old instances stay permanently fenced and cannot
 *       ride the in-progress path back in, while a fresh instance of the same
 *       id takes over cleanly.</li>
 * </ul>
 */
class InterleavedMembershipTest {

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

    @Test
    void syncOfSupersededWindowIsInProgressNotFinalFailure() throws Exception {
        GroupCoordinator c = newCoordinator(4, 500);
        JoinResult a = bootstrap("a");
        assertEquals(JoinStatus.STABLE, a.status());

        // b joins: window gen2 opens. Before b finishes phase 2, c joins and
        // supersedes it with window gen3. b is now "in the earlier round".
        JoinResult bJoin = c.joinGroup("b", null);
        JoinResult cJoin = c.joinGroup("c", null);
        assertTrue(cJoin.generation() > bJoin.generation());
        assertEquals(GroupState.REBALANCING, c.state());

        // b confirms the generation it had joined: superseded, but b is alive
        // and current -> retryable IN_PROGRESS for the window open now, NOT a
        // final STALE_GENERATION failure.
        JoinResult bSync = c.syncGroup("b", bJoin.instanceToken(), bJoin.generation());
        assertEquals(JoinStatus.IN_PROGRESS, bSync.status());
        assertEquals(cJoin.generation(), bSync.generation(),
                "in-progress result must carry the generation open now");

        // Everyone converges on the open generation; the window closes.
        c.syncGroup("c", cJoin.instanceToken(), cJoin.generation());
        c.joinGroup("a", a.instanceToken());
        JoinResult aStable = c.syncGroup("a", a.instanceToken(), cJoin.generation());
        assertEquals(JoinStatus.STABLE, aStable.status());

        // b keeps confirming with the newest generation and gets its share.
        JoinResult bStable = c.syncGroup("b", bJoin.instanceToken(), bSync.generation());
        JoinResult cStable = c.syncGroup("c", cJoin.instanceToken(), cJoin.generation());
        assertEquals(JoinStatus.STABLE, bStable.status());
        assertEquals(JoinStatus.STABLE, cStable.status());
        assertEquals(Set.of("a", "b", "c"), c.members());
        assertCompleteAndDisjoint(c, 4);
        assertTrue(c.verifyNoOwnershipOverlap());

        // All three can keep committing on their own partitions.
        for (JoinResult r : new JoinResult[]{aStable, bStable, cStable}) {
            for (Partition p : r.assignment()) {
                c.commit(r == aStable ? "a" : r == bStable ? "b" : "c",
                        r.instanceToken(), r.generation(), p, 0);
            }
        }
        c.close();
    }

    @Test
    void simultaneousJoinsSupersedeChainAndConverge() throws Exception {
        GroupCoordinator c = newCoordinator(6, 500);
        // Three members join back-to-back; each join supersedes the previous
        // window before anybody finished phase 2.
        JoinResult aJoin = c.joinGroup("a", null);
        JoinResult bJoin = c.joinGroup("b", null);
        JoinResult cJoin = c.joinGroup("c", null);
        assertTrue(bJoin.generation() > aJoin.generation());
        assertTrue(cJoin.generation() > bJoin.generation());

        // a and b confirm the (superseded) generations they had joined: both
        // are absorbed into the open window and told IN_PROGRESS, not failed.
        JoinResult aSync = c.syncGroup("a", aJoin.instanceToken(), aJoin.generation());
        JoinResult bSync = c.syncGroup("b", bJoin.instanceToken(), bJoin.generation());
        assertEquals(JoinStatus.IN_PROGRESS, aSync.status());
        assertEquals(JoinStatus.IN_PROGRESS, bSync.status());
        assertEquals(cJoin.generation(), aSync.generation());
        assertEquals(cJoin.generation(), bSync.generation());

        // c confirms the open generation: everybody synced, window closes.
        JoinResult cSync = c.syncGroup("c", cJoin.instanceToken(), cJoin.generation());
        assertEquals(JoinStatus.STABLE, cSync.status());
        assertFalse(cSync.completedByTimeout());

        // a and b re-confirm with the newest generation and get their shares.
        JoinResult aStable = c.syncGroup("a", aJoin.instanceToken(), aSync.generation());
        JoinResult bStable = c.syncGroup("b", bJoin.instanceToken(), bSync.generation());
        assertEquals(JoinStatus.STABLE, aStable.status());
        assertEquals(JoinStatus.STABLE, bStable.status());
        assertEquals(2, aStable.assignment().size());
        assertEquals(2, bStable.assignment().size());
        assertEquals(2, cSync.assignment().size());
        assertCompleteAndDisjoint(c, 6);
        assertTrue(c.verifyNoOwnershipOverlap());
        c.close();
    }

    @Test
    void staleSyncOfLiveMemberAfterCloseReturnsCurrentAssignment() throws Exception {
        // A member whose confirming request lands after its generation was
        // closed AND superseded (e.g. an expiry-driven rebalance in between)
        // is still registered and current: it must learn the current
        // assignment, not be failed.
        GroupCoordinator c = newCoordinator(4, 300, 500);
        JoinResult a = bootstrap("a");
        JoinResult bJoin = c.joinGroup("b", null);
        c.joinGroup("a", a.instanceToken());
        JoinResult b = c.syncGroup("b", bJoin.instanceToken(), bJoin.generation());
        JoinResult a2 = c.syncGroup("a", a.instanceToken(), bJoin.generation());
        assertEquals(JoinStatus.STABLE, a2.status());
        int stableGen = a2.generation();

        // b goes silent and expires; a keeps heartbeating. The expiry triggers
        // a synchronous rebalance that advances the generation while a's
        // knowledge stays behind.
        clock.advance(200);
        c.heartbeat("a", a.instanceToken(), stableGen);
        clock.advance(200); // b idle 400 > sessionTimeout(300); a idle 200
        c.checkExpirations();
        assertEquals(Set.of("a"), c.members());
        assertTrue(c.generation() > stableGen);

        // a confirms the generation it last knew: it is live and current, so
        // the coordinator answers with the CURRENT stable assignment.
        JoinResult aSync = c.syncGroup("a", a.instanceToken(), stableGen);
        assertEquals(JoinStatus.STABLE, aSync.status());
        assertEquals(c.generation(), aSync.generation());
        assertEquals(4, aSync.assignment().size());
        assertCompleteAndDisjoint(c, 4);

        // The evicted b is permanently fenced, not on the in-progress path.
        CoordinatorException e = assertThrows(CoordinatorException.class,
                () -> c.syncGroup("b", b.instanceToken(), bJoin.generation()));
        assertEquals(RejectionReason.FENCED_INSTANCE, e.reason());
        c.close();
    }

    @Test
    void fencedOldInstanceCannotRideInProgressPath() throws Exception {
        GroupCoordinator c = newCoordinator(2, 500);
        JoinResult oldA = bootstrap("a");
        Partition p0 = new Partition("topic", 0);
        c.commit("a", oldA.instanceToken(), oldA.generation(), p0, 5);

        // Interleaving: b joins (window opens), then a NEW instance of a
        // restarts (blank token) and takes over the identity inside the open
        // window, retiring the old instance token.
        JoinResult bJoin = c.joinGroup("b", null);
        JoinResult neoA = c.joinGroup("a", null);
        assertNotEquals(oldA.instanceToken(), neoA.instanceToken());
        assertEquals(JoinStatus.IN_PROGRESS, neoA.status());
        assertEquals(GroupState.REBALANCING, c.state());

        // The retired instance asks to confirm / observe / commit: every path
        // is a permanent FENCED_INSTANCE, never IN_PROGRESS.
        CoordinatorException sync = assertThrows(CoordinatorException.class,
                () -> c.syncGroup("a", oldA.instanceToken(), oldA.generation()));
        assertEquals(RejectionReason.FENCED_INSTANCE, sync.reason());
        CoordinatorException now = assertThrows(CoordinatorException.class,
                () -> c.syncCurrent("a", oldA.instanceToken()));
        assertEquals(RejectionReason.FENCED_INSTANCE, now.reason());
        CoordinatorException commit = assertThrows(CoordinatorException.class,
                () -> c.commit("a", oldA.instanceToken(), neoA.generation(), p0, 6));
        assertEquals(RejectionReason.FENCED_INSTANCE, commit.reason());
        assertEquals(5, c.fetchCommitted(p0), "fenced commit must not move the offset");

        // The new instance and b close the window; the new instance of a
        // cleanly owns its share and progresses.
        c.joinGroup("b", bJoin.instanceToken());
        c.syncGroup("b", bJoin.instanceToken(), neoA.generation());
        JoinResult neoStable = c.syncGroup("a", neoA.instanceToken(), neoA.generation());
        JoinResult bStable = c.syncGroup("b", bJoin.instanceToken(), neoA.generation());
        assertEquals(JoinStatus.STABLE, bStable.status());
        assertEquals(JoinStatus.STABLE, neoStable.status());
        assertCompleteAndDisjoint(c, 2);
        for (Partition p : neoStable.assignment()) {
            c.commit("a", neoA.instanceToken(), neoStable.generation(), p,
                    c.fetchCommitted(p) + 1);
        }
        // Old instance stays fenced after everything settles.
        CoordinatorException still = assertThrows(CoordinatorException.class,
                () -> c.heartbeat("a", oldA.instanceToken(), neoStable.generation()));
        assertEquals(RejectionReason.FENCED_INSTANCE, still.reason());
        c.close();
    }

    @Test
    void supersededWindowStillClosesDeterministicallyAtBound() throws Exception {
        GroupCoordinator c = newCoordinator(4, 300);
        JoinResult a = bootstrap("a");
        // b joins, c supersedes; b gets absorbed into the open window, but a
        // never participates and never renews.
        JoinResult bJoin = c.joinGroup("b", null);
        JoinResult cJoin = c.joinGroup("c", null);
        JoinResult bSync = c.syncGroup("b", bJoin.instanceToken(), bJoin.generation());
        assertEquals(JoinStatus.IN_PROGRESS, bSync.status());
        assertEquals(cJoin.generation(), bSync.generation());

        // Before the bound: still observably in progress (not a final answer).
        clock.advance(299);
        JoinResult waiting = c.syncGroup("b", bJoin.instanceToken(), cJoin.generation());
        assertEquals(JoinStatus.IN_PROGRESS, waiting.status());

        // At the bound the window closes deterministically: a and c (silent,
        // never synced) are evicted; b survives with the full assignment and a
        // definite timeout-flagged conclusion.
        clock.advance(2);
        JoinResult closed = c.syncGroup("b", bJoin.instanceToken(), cJoin.generation());
        assertEquals(JoinStatus.STABLE, closed.status());
        assertTrue(closed.completedByTimeout());
        assertEquals(Set.of("b"), c.members());
        assertEquals(4, closed.assignment().size());
        assertCompleteAndDisjoint(c, 4);

        // The evicted members get a definite final answer, not IN_PROGRESS.
        CoordinatorException cSync = assertThrows(CoordinatorException.class,
                () -> c.syncGroup("c", cJoin.instanceToken(), cJoin.generation()));
        assertEquals(RejectionReason.FENCED_INSTANCE, cSync.reason());
        CoordinatorException aHb = assertThrows(CoordinatorException.class,
                () -> c.heartbeat("a", a.instanceToken(), a.generation()));
        assertEquals(RejectionReason.FENCED_INSTANCE, aHb.reason());
        c.close();
    }
}
