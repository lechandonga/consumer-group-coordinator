package com.github.lechandonga.cgc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simulated consumer process: heartbeats on a schedule, tracks its assignment,
 * commits processed offsets, and rejoins when fenced by a newer generation.
 * Can be killed (simulated crash: heartbeats stop without leave) and restarted
 * with the same id, resuming from the last committed offsets.
 */
public class ConsumerMember implements AutoCloseable {

    private final String memberId;
    private final GroupCoordinator coordinator;
    private final long heartbeatIntervalMs;

    private final Object lock = new Object();
    private int generation = -1;
    private Set<Partition> assignment = Set.of();
    private final Map<Partition, Long> positions = new HashMap<>();

    private final AtomicInteger commitRejections = new AtomicInteger();
    private volatile Thread heartbeatThread;
    private volatile boolean running;

    public ConsumerMember(String memberId, GroupCoordinator coordinator) {
        this.memberId = memberId;
        this.coordinator = coordinator;
        this.heartbeatIntervalMs = coordinator.config().heartbeatIntervalMs();
    }

    /** Starts the member: joins the group and begins the heartbeat loop. */
    public void start() {
        synchronized (lock) {
            if (running) {
                return;
            }
            running = true;
        }
        rejoin();
        Thread t = new Thread(this::heartbeatLoop, "consumer-member-" + memberId);
        t.setDaemon(true);
        heartbeatThread = t;
        t.start();
    }

    private void rejoin() {
        HeartbeatResponse response = coordinator.join(memberId);
        synchronized (lock) {
            adoptAssignment(response);
        }
    }

    /** Must be called with lock held. */
    private void adoptAssignment(HeartbeatResponse response) {
        generation = response.generation();
        assignment = response.assignment();
        // Resume from the last confirmed committed offsets (crash recovery).
        for (Partition p : assignment) {
            positions.put(p, coordinator.fetchCommitted(p));
        }
        positions.keySet().retainAll(assignment);
    }

    private void heartbeatLoop() {
        while (running) {
            try {
                Thread.sleep(heartbeatIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (!running) {
                return;
            }
            int gen;
            synchronized (lock) {
                gen = generation;
            }
            try {
                HeartbeatResponse response = coordinator.heartbeat(memberId, gen);
                synchronized (lock) {
                    adoptAssignment(response);
                }
            } catch (CoordinatorException e) {
                switch (e.reason()) {
                    case STALE_GENERATION, ILLEGAL_GENERATION, UNKNOWN_MEMBER -> rejoin();
                    case REBALANCE_IN_PROGRESS -> { /* retry next tick */ }
                    default -> { /* NOT_OWNER / OFFSET_REGRESSION cannot happen here */ }
                }
            }
        }
    }

    /** Simulates a crash: stops heartbeating without leaving the group. */
    public void kill() {
        stopLoop();
    }

    /** Voluntary, clean shutdown (sends leave). */
    public void shutdown() {
        stopLoop();
        try {
            coordinator.leave(memberId);
        } catch (CoordinatorException ignored) {
            // Already expired or unknown: nothing to clean up.
        }
    }

    private void stopLoop() {
        running = false;
        Thread t = heartbeatThread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            heartbeatThread = null;
        }
    }

    /**
     * Simulates processing one record per owned partition and committing the
     * new position. Fenced commits (stale generation / lost ownership) are
     * rejected by the coordinator and leave local state untouched.
     */
    public void pollOnce() {
        int gen;
        Set<Partition> owned;
        synchronized (lock) {
            gen = generation;
            owned = assignment;
        }
        for (Partition p : owned) {
            long next;
            synchronized (lock) {
                next = positions.getOrDefault(p, -1L) + 1;
            }
            try {
                coordinator.commit(memberId, gen, p, next);
                synchronized (lock) {
                    positions.put(p, next);
                }
            } catch (CoordinatorException e) {
                commitRejections.incrementAndGet();
                if (e.reason() == RejectionReason.STALE_GENERATION
                        || e.reason() == RejectionReason.UNKNOWN_MEMBER
                        || e.reason() == RejectionReason.ILLEGAL_GENERATION) {
                    rejoin();
                    return;
                }
            }
        }
    }

    public String memberId() {
        return memberId;
    }

    public Set<Partition> currentAssignment() {
        synchronized (lock) {
            return assignment;
        }
    }

    /** Local (in-memory) positions per partition, as this member sees them. */
    public Map<Partition, Long> localPositions() {
        synchronized (lock) {
            return Collections.unmodifiableMap(new HashMap<>(positions));
        }
    }

    /** Number of commits rejected by the coordinator (fencing working). */
    public int commitRejections() {
        return commitRejections.get();
    }

    @Override
    public void close() {
        shutdown();
    }
}
