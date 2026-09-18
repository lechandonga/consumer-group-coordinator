package com.github.lechandonga.cgc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Crash-recovery scenarios: a member dies without leaving, gets evicted after
 * the session timeout, and a process restarting with the same id resumes from
 * the last confirmed committed offsets.
 */
class CrashRecoveryTest {

    @TempDir
    Path dir;

    private GroupCoordinator coordinator;

    private GroupCoordinator newCoordinator(int partitions) {
        CoordinatorConfig config = new CoordinatorConfig(
                "topic", partitions, 20, 200, 400, dir.resolve("offsets.log"));
        coordinator = new GroupCoordinator(config); // wall clock + auto expiry
        return coordinator;
    }

    private static void await(BooleanSupplier condition, long timeoutMs, String what)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("timed out waiting for: " + what);
            }
            Thread.sleep(10);
        }
    }

    @Test
    void restartedMemberResumesFromCommittedOffsets() throws Exception {
        GroupCoordinator c = newCoordinator(2);
        Partition p0 = new Partition("topic", 0);
        Partition p1 = new Partition("topic", 1);

        ConsumerMember member = new ConsumerMember("m1", c);
        member.start();
        await(() -> member.currentAssignment().size() == 2, 2000, "initial assignment");
        for (int i = 0; i < 5; i++) {
            member.pollOnce();
        }
        Map<Partition, Long> committedBeforeCrash = member.localPositions();
        assertEquals(Map.of(p0, 4L, p1, 4L), committedBeforeCrash);
        member.kill(); // simulated crash: no leave, heartbeats stop

        // Session expires -> member evicted -> group empty.
        await(() -> c.members().isEmpty(), 5000, "member eviction after crash");

        // Process restarts with the same id and a fresh in-memory state.
        ConsumerMember restarted = new ConsumerMember("m1", c);
        restarted.start();
        await(() -> restarted.currentAssignment().size() == 2, 2000, "assignment after restart");
        assertEquals(committedBeforeCrash, restarted.localPositions(),
                "restarted process must resume from the last confirmed committed offsets");
        assertEquals(committedBeforeCrash.get(p0), (Long) c.fetchCommitted(p0));
        assertEquals(committedBeforeCrash.get(p1), (Long) c.fetchCommitted(p1));

        // And it can keep making progress from there.
        restarted.pollOnce();
        assertEquals(committedBeforeCrash.get(p0) + 1, (Long) c.fetchCommitted(p0));
        restarted.shutdown();
        c.close();
    }

    @Test
    void survivorKeepsProgressingWhileCrashedMemberIsEvicted() throws Exception {
        GroupCoordinator c = newCoordinator(4);
        ConsumerMember a = new ConsumerMember("a", c);
        ConsumerMember b = new ConsumerMember("b", c);
        a.start();
        b.start();
        await(() -> !a.currentAssignment().isEmpty() && !b.currentAssignment().isEmpty(),
                2000, "both members assigned");

        a.kill(); // a crashes
        // b must end up owning all partitions after a's session expires.
        await(() -> b.currentAssignment().size() == 4, 5000, "b takes over all partitions");
        b.pollOnce();
        assertTrue(c.verifyNoOwnershipOverlap());

        // a's process restarts; group rebalances again, coverage stays complete.
        ConsumerMember a2 = new ConsumerMember("a", c);
        a2.start();
        await(() -> !a2.currentAssignment().isEmpty(), 5000, "a reassigned after restart");
        assertTrue(c.verifyNoOwnershipOverlap());
        long total = 0;
        for (int i = 0; i < 4; i++) {
            total += c.fetchCommitted(new Partition("topic", i)) + 1;
        }
        assertTrue(total > 0, "progress was made and preserved");
        a2.shutdown();
        b.shutdown();
        c.close();
    }
}
