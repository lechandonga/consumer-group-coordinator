package com.github.lechandonga.cgc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GroupCoordinatorTest {

    @TempDir
    Path dir;

    private MutableClock clock;
    private GroupCoordinator coordinator;

    private GroupCoordinator newCoordinator(int partitions) {
        clock = new MutableClock(0);
        CoordinatorConfig config = new CoordinatorConfig(
                "topic", partitions, 100, 1000, 2000, dir.resolve("offsets-" + System.nanoTime() + ".log"));
        coordinator = new GroupCoordinator(config, clock);
        return coordinator;
    }

    private static void assertCompleteAndDisjoint(GroupCoordinator c, int partitionCount) {
        Map<Partition, String> ownership = c.ownershipSnapshot();
        assertEquals(partitionCount, ownership.size(), "every partition must have exactly one owner");
        Set<String> owners = new HashSet<>(ownership.values());
        // Rebuild per-member view and check disjointness.
        Map<String, Set<Partition>> byMember = new HashMap<>();
        ownership.forEach((p, m) -> byMember.computeIfAbsent(m, k -> new HashSet<>()).add(p));
        int total = byMember.values().stream().mapToInt(Set::size).sum();
        assertEquals(partitionCount, total, "assignments must not overlap");
        assertEquals(c.members(), byMember.keySet().isEmpty() ? c.members() : byMember.keySet());
        assertTrue(owners.size() <= c.members().size());
    }

    @Test
    void joinAssignsAllPartitionsToSingleMember() throws Exception {
        GroupCoordinator c = newCoordinator(4);
        HeartbeatResponse r = c.join("m1");
        assertEquals(GroupState.STABLE, c.state());
        assertEquals(4, r.assignment().size());
        assertCompleteAndDisjoint(c, 4);
        assertTrue(c.verifyNoOwnershipOverlap());
        c.close();
    }

    @Test
    void secondMemberTriggersRebalanceWithNewGeneration() {
        GroupCoordinator c = newCoordinator(4);
        int gen1 = c.join("m1").generation();
        HeartbeatResponse r2 = c.join("m2");
        assertTrue(r2.generation() > gen1, "generation must advance on membership change");
        assertEquals(2, c.assignmentOf("m1").size());
        assertEquals(2, c.assignmentOf("m2").size());
        assertCompleteAndDisjoint(c, 4);
        assertTrue(c.verifyNoOwnershipOverlap());
        c.close();
    }

    @Test
    void leaveRebalancesRemainingMembers() throws Exception {
        GroupCoordinator c = newCoordinator(3);
        c.join("m1");
        c.join("m2");
        c.leave("m2");
        assertEquals(3, c.assignmentOf("m1").size());
        assertCompleteAndDisjoint(c, 3);
        assertTrue(c.verifyNoOwnershipOverlap());
        c.close();
    }

    @Test
    void expiredMemberIsEvictedAndPartitionsReassigned() throws Exception {
        GroupCoordinator c = newCoordinator(4);
        c.join("m1");
        c.join("m2");
        int genBefore = c.generation();
        // m1 stays alive, m2 stops heartbeating.
        clock.advance(500);
        c.heartbeat("m1", c.generation());
        clock.advance(600); // m2 idle for 1100ms > sessionTimeout, m1 idle for 600ms
        c.checkExpirations();
        assertEquals(Set.of("m1"), c.members());
        assertTrue(c.generation() > genBefore);
        assertEquals(4, c.assignmentOf("m1").size());
        assertCompleteAndDisjoint(c, 4);
        assertTrue(c.verifyNoOwnershipOverlap());
        c.close();
    }

    @Test
    void liveMemberIsNotExpired() throws Exception {
        GroupCoordinator c = newCoordinator(2);
        c.join("m1");
        int gen = c.generation();
        clock.advance(500);
        c.heartbeat("m1", gen);
        clock.advance(900);
        c.checkExpirations();
        assertEquals(Set.of("m1"), c.members());
        assertEquals(gen, c.generation());
        c.close();
    }

    @Test
    void staleGenerationHeartbeatRejected() throws Exception {
        GroupCoordinator c = newCoordinator(2);
        int gen1 = c.join("m1").generation();
        c.join("m2");
        CoordinatorException e = assertThrows(CoordinatorException.class,
                () -> c.heartbeat("m1", gen1));
        assertEquals(RejectionReason.STALE_GENERATION, e.reason());
        c.close();
    }

    @Test
    void illegalFutureGenerationRejected() throws Exception {
        GroupCoordinator c = newCoordinator(2);
        c.join("m1");
        CoordinatorException e = assertThrows(CoordinatorException.class,
                () -> c.heartbeat("m1", c.generation() + 5));
        assertEquals(RejectionReason.ILLEGAL_GENERATION, e.reason());
        c.close();
    }

    @Test
    void unknownMemberRejected() {
        GroupCoordinator c = newCoordinator(2);
        c.join("m1");
        CoordinatorException e = assertThrows(CoordinatorException.class,
                () -> c.heartbeat("ghost", c.generation()));
        assertEquals(RejectionReason.UNKNOWN_MEMBER, e.reason());
        c.close();
    }

    @Test
    void staleGenerationCommitRejectedAndStateUnchanged() throws Exception {
        GroupCoordinator c = newCoordinator(2);
        int gen1 = c.join("m1").generation();
        Partition p0 = new Partition("topic", 0);
        c.commit("m1", gen1, p0, 10);
        c.join("m2"); // advances generation, m1 may lose p0
        CoordinatorException e = assertThrows(CoordinatorException.class,
                () -> c.commit("m1", gen1, p0, 11));
        assertEquals(RejectionReason.STALE_GENERATION, e.reason());
        assertEquals(10, c.fetchCommitted(p0), "rejected commit must not change state");
        c.close();
    }

    @Test
    void commitForUnownedPartitionRejected() throws Exception {
        GroupCoordinator c = newCoordinator(2);
        c.join("m1");
        c.join("m2");
        int gen = c.generation();
        // Find a partition owned by m2 and try to commit it as m1.
        Partition foreign = c.ownershipSnapshot().entrySet().stream()
                .filter(e -> e.getValue().equals("m2")).findFirst().orElseThrow().getKey();
        CoordinatorException e = assertThrows(CoordinatorException.class,
                () -> c.commit("m1", gen, foreign, 5));
        assertEquals(RejectionReason.NOT_OWNER, e.reason());
        assertEquals(-1, c.fetchCommitted(foreign));
        c.close();
    }

    @Test
    void offsetRegressionRejected() throws Exception {
        GroupCoordinator c = newCoordinator(1);
        int gen = c.join("m1").generation();
        Partition p0 = new Partition("topic", 0);
        c.commit("m1", gen, p0, 10);
        CoordinatorException e = assertThrows(CoordinatorException.class,
                () -> c.commit("m1", gen, p0, 4));
        assertEquals(RejectionReason.OFFSET_REGRESSION, e.reason());
        assertEquals(10, c.fetchCommitted(p0));
        c.close();
    }

    @Test
    void lastMemberLeavingEmptiesGroup() throws Exception {
        GroupCoordinator c = newCoordinator(2);
        c.join("m1");
        c.leave("m1");
        assertEquals(GroupState.EMPTY, c.state());
        assertTrue(c.ownershipSnapshot().isEmpty());
        assertTrue(c.verifyNoOwnershipOverlap());
        c.close();
    }
}
