package com.github.lechandonga.cgc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Interleaving storm: members poll and commit concurrently while a chaos
 * thread kills/restarts members and triggers rebalances. The invariants under
 * test: no ownership overlap ever, committed offsets never regress, and no
 * committed position is ever lost (a member's local position can never exceed
 * the committed offset for that partition).
 */
class ConcurrencyTest {

    @TempDir
    Path dir;

    @Test
    void concurrentCommitsRebalancesAndRestarts() throws Exception {
        int partitionCount = 4;
        CoordinatorConfig config = new CoordinatorConfig(
                "topic", partitionCount, 20, 200, 400, dir.resolve("offsets.log"));
        try (GroupCoordinator coordinator = new GroupCoordinator(config)) {
            AtomicBoolean running = new AtomicBoolean(true);
            List<ConsumerMember> members = new CopyOnWriteArrayList<>();
            for (int i = 0; i < 3; i++) {
                ConsumerMember m = new ConsumerMember("m" + i, coordinator);
                m.start();
                members.add(m);
            }

            // Watcher: committed offsets must never regress.
            Map<Partition, Long> highWater = new ConcurrentHashMap<>();
            List<String> regressions = new CopyOnWriteArrayList<>();
            Thread watcher = new Thread(() -> {
                while (running.get()) {
                    for (int i = 0; i < partitionCount; i++) {
                        Partition p = new Partition("topic", i);
                        long committed = coordinator.fetchCommitted(p);
                        highWater.merge(p, committed, Math::max);
                        if (committed < highWater.get(p)) {
                            regressions.add(p + " regressed to " + committed
                                    + " from " + highWater.get(p));
                        }
                    }
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            });
            watcher.setDaemon(true);
            watcher.start();

            // Pollers: each member polls from its own thread.
            CountDownLatch stop = new CountDownLatch(1);
            List<Thread> pollers = new ArrayList<>();
            for (ConsumerMember m : members) {
                Thread t = new Thread(() -> {
                    while (running.get()) {
                        m.pollOnce();
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                });
                t.setDaemon(true);
                t.start();
                pollers.add(t);
            }

            // Chaos: deterministic seed for reproducibility.
            Random rnd = new Random(42);
            long deadline = System.currentTimeMillis() + 3000;
            while (System.currentTimeMillis() < deadline) {
                Thread.sleep(120);
                ConsumerMember victim = members.get(rnd.nextInt(members.size()));
                switch (rnd.nextInt(3)) {
                    case 0 -> victim.kill();      // crash -> session expiry -> rebalance
                    case 1 -> victim.start();     // restart (rejoin)
                    case 2 -> {                   // extra member joins, then leaves
                        ConsumerMember extra = new ConsumerMember("extra", coordinator);
                        extra.start();
                        Thread.sleep(80);
                        extra.shutdown();
                    }
                    default -> throw new IllegalStateException();
                }
            }

            // Drain: make sure everyone is alive and heartbeating again.
            for (ConsumerMember m : members) {
                m.start();
            }
            Thread.sleep(500);
            running.set(false);
            stop.countDown();
            for (Thread t : pollers) {
                t.join(2000);
            }
            watcher.join(2000);

            // Invariant 1: committed offsets never regressed.
            assertTrue(regressions.isEmpty(), "offset regressions: " + regressions);

            // Invariant 2: no partition ever had two simultaneous owners.
            assertTrue(coordinator.verifyNoOwnershipOverlap());

            // Invariant 3: final assignment covers all partitions disjointly.
            assertEquals(partitionCount, coordinator.ownershipSnapshot().size());

            // Invariant 4: no lost updates — a member's local position can
            // never be ahead of the committed offset for that partition.
            for (ConsumerMember m : members) {
                for (Map.Entry<Partition, Long> e : m.localPositions().entrySet()) {
                    long committed = coordinator.fetchCommitted(e.getKey());
                    assertTrue(e.getValue() <= committed,
                            m.memberId() + " position " + e.getValue() + " for " + e.getKey()
                                    + " exceeds committed " + committed);
                }
            }

            // Invariant 5: the group actually made durable progress.
            long total = 0;
            for (int i = 0; i < partitionCount; i++) {
                total += coordinator.fetchCommitted(new Partition("topic", i));
            }
            assertTrue(total > 0, "no progress was committed");

            for (ConsumerMember m : members) {
                m.shutdown();
            }
        }
    }

    @Test
    void commitsRacingRebalanceAreFencedAtomically() throws Exception {
        // Deterministic interleaving: a commit carrying the old generation
        // must either land entirely before the rebalance or be rejected after
        // it; the committed state is never half-updated.
        CoordinatorConfig config = new CoordinatorConfig(
                "topic", 1, 20, 200, 400, dir.resolve("race.log"));
        try (GroupCoordinator coordinator = new GroupCoordinator(config)) {
            Partition p0 = new Partition("topic", 0);
            int gen = coordinator.join("m1").generation();

            int rounds = 200;
            List<String> anomalies = new CopyOnWriteArrayList<>();
            for (int i = 0; i < rounds; i++) {
                long before = coordinator.fetchCommitted(p0);
                int currentGen = coordinator.generation();
                Thread committer = new Thread(() -> {
                    try {
                        coordinator.commit("m1", currentGen, p0, before + 1);
                    } catch (CoordinatorException e) {
                        // Fencing rejections are expected and fine.
                    }
                });
                Thread rebalancer = new Thread(() -> {
                    coordinator.join("m2");
                    try {
                        coordinator.leave("m2");
                    } catch (CoordinatorException ignored) {
                    }
                });
                committer.start();
                rebalancer.start();
                committer.join(2000);
                rebalancer.join(2000);
                long after = coordinator.fetchCommitted(p0);
                if (after != before && after != before + 1) {
                    anomalies.add("round " + i + ": committed jumped " + before + " -> " + after);
                }
                if (after < before) {
                    anomalies.add("round " + i + ": regression " + before + " -> " + after);
                }
                // m1 re-syncs its generation for the next round.
                coordinator.heartbeat("m1", coordinator.generation());
            }
            assertTrue(anomalies.isEmpty(), () -> "anomalies: " + anomalies);
            assertTrue(coordinator.verifyNoOwnershipOverlap());
        }
    }
}
