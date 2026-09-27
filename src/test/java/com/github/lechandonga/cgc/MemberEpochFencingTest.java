package com.github.lechandonga.cgc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Member-instance fencing: when a member id is taken over by a new instance,
 * the replaced instance's heartbeats and commits must be rejected observably
 * and must not disturb the new instance's progress.
 */
class MemberEpochFencingTest {

    @TempDir
    Path dir;

    private GroupCoordinator newCoordinator(int partitions, MutableClock clock) {
        CoordinatorConfig config = new CoordinatorConfig(
                "topic", partitions, 100, 1000, 2000,
                dir.resolve("offsets-" + System.nanoTime() + ".log"));
        return new GroupCoordinator(config, clock);
    }

    @Test
    void replacedInstanceIsFencedAndNewInstanceTakesOver() throws Exception {
        MutableClock clock = new MutableClock(0);
        GroupCoordinator c = newCoordinator(2, clock);
        HeartbeatResponse first = c.join("m1");
        HeartbeatResponse second = c.join("m1"); // new instance, same identity
        assertTrue(second.memberEpoch() > first.memberEpoch(),
                "every join must issue a fresh, larger epoch");

        // Old instance is rejected, distinguishably.
        CoordinatorException hb = assertThrows(CoordinatorException.class,
                () -> c.heartbeat("m1", first.memberEpoch(), first.generation()));
        assertEquals(RejectionReason.FENCED_MEMBER_EPOCH, hb.reason());

        Partition p0 = new Partition("topic", 0);
        CoordinatorException commit = assertThrows(CoordinatorException.class,
                () -> c.commit("m1", first.memberEpoch(), first.generation(), p0, 5));
        assertEquals(RejectionReason.FENCED_MEMBER_EPOCH, commit.reason());
        assertEquals(-1, c.fetchCommitted(p0), "fenced commit must not change state");

        // New instance takes over the same identity's duties cleanly.
        HeartbeatResponse ok = c.heartbeat("m1", second.memberEpoch(), second.generation());
        assertEquals(2, ok.assignment().size());
        c.commit("m1", second.memberEpoch(), second.generation(), p0, 5);
        assertEquals(5, c.fetchCommitted(p0));
        c.close();
    }

    @Test
    void fencedInstanceCannotOverwriteNewInstancesProgress() throws Exception {
        MutableClock clock = new MutableClock(0);
        GroupCoordinator c = newCoordinator(1, clock);
        HeartbeatResponse first = c.join("m1");
        HeartbeatResponse second = c.join("m1");
        Partition p0 = new Partition("topic", 0);
        c.commit("m1", second.memberEpoch(), second.generation(), p0, 10);

        // The fenced instance still believes it owns the partition; its
        // (regressing) commit is rejected by epoch fencing before any
        // offset check, and the committed offset is untouched.
        CoordinatorException e = assertThrows(CoordinatorException.class,
                () -> c.commit("m1", first.memberEpoch(), first.generation(), p0, 3));
        assertEquals(RejectionReason.FENCED_MEMBER_EPOCH, e.reason());
        assertEquals(10, c.fetchCommitted(p0));
        c.close();
    }

    @Test
    void consumerMemberStopsWhenReplacedByNewInstance() throws Exception {
        MutableClock clock = new MutableClock(0);
        CoordinatorConfig config = new CoordinatorConfig(
                "topic", 2, 20, 200, 400, dir.resolve("offsets-member.log"));
        try (GroupCoordinator c = new GroupCoordinator(config, clock)) {
            ConsumerMember oldInstance = new ConsumerMember("m1", c);
            oldInstance.start();
            await(() -> !oldInstance.currentAssignment().isEmpty());

            ConsumerMember newInstance = new ConsumerMember("m1", c);
            newInstance.start();

            // The old instance observes FENCED_MEMBER_EPOCH and stops.
            await(oldInstance::isFenced);

            // The new instance keeps progressing on the taken-over partitions.
            for (int i = 0; i < 3; i++) {
                newInstance.pollOnce();
            }
            await(() -> newInstance.localPositions().values().stream()
                    .allMatch(o -> o >= 2));
            assertTrue(newInstance.commitRejections() == 0,
                    "new instance must not be fenced by the old one");
            newInstance.shutdown();
        }
    }

    private static void await(BooleanCondition condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("condition not met within 10s");
            }
            Thread.sleep(10);
        }
    }

    @FunctionalInterface
    private interface BooleanCondition {
        boolean getAsBoolean();
    }
}
