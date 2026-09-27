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
 *
 * <p>Every member instance carries the member epoch issued by its latest
 * join. If another instance of the same member id joins, this instance's
 * requests are rejected with {@link RejectionReason#FENCED_MEMBER_EPOCH};
 * that rejection is terminal for this instance (it stops instead of fighting
 * the replacement), which makes "the replaced instance was rejected" directly
 * observable via {@link #isFenced()}.
 *
 * <p>Works against any {@link Coordinator}: the embedded
 * {@link GroupCoordinator} or a remote coordinator server via a client.
 */
public class ConsumerMember implements AutoCloseable {

    private final String memberId;
    private final Coordinator coordinator;
    private final long heartbeatIntervalMs;

    private final Object lock = new Object();
    private int generation = -1;
    private long memberEpoch = -1;
    private Set<Partition> assignment = Set.of();
    private final Map<Partition, Long> positions = new HashMap<>();
    private boolean fenced;

    private final AtomicInteger commitRejections = new AtomicInteger();
    private volatile Thread heartbeatThread;
    private volatile boolean running;

    public ConsumerMember(String memberId, Coordinator coordinator) {
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
        HeartbeatResponse response;
        try {
            response = coordinator.joinAndAwait(memberId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (CoordinatorException e) {
            return; // evicted while waiting; the heartbeat loop will retry
        }
        synchronized (lock) {
            adoptAssignment(response);
        }
    }

    /** Must be called with lock held. */
    private void adoptAssignment(HeartbeatResponse response) {
        generation = response.generation();
        memberEpoch = response.memberEpoch();
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
            long epoch;
            synchronized (lock) {
                gen = generation;
                epoch = memberEpoch;
            }
            try {
                HeartbeatResponse response = coordinator.heartbeat(memberId, epoch, gen);
                synchronized (lock) {
                    adoptAssignment(response);
                }
            } catch (CoordinatorException e) {
                switch (e.reason()) {
                    case STALE_GENERATION, ILLEGAL_GENERATION, UNKNOWN_MEMBER -> rejoin();
                    case REBALANCE_IN_PROGRESS -> rejoin(); // confirm the open barrier
                    case FENCED_MEMBER_EPOCH -> {
                        if (markFencedIfCurrent(epoch)) {
                            return; // replaced by a newer instance: stop, do not fight it
                        }
                    }
                    default -> { /* NOT_OWNER / OFFSET_REGRESSION cannot happen here */ }
                }
            }
        }
    }

    /**
     * Marks this instance fenced if the rejected epoch is still the current
     * one (a concurrent local rejoin may already have rotated it). Returns
     * true when the instance was indeed fenced and must stop.
     */
    private boolean markFencedIfCurrent(long rejectedEpoch) {
        synchronized (lock) {
            if (rejectedEpoch != memberEpoch) {
                return false; // stale in-flight request; local epoch already rotated
            }
            fenced = true;
        }
        stopLoop();
        return true;
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
        if (t != null && t != Thread.currentThread()) {
            t.interrupt();
            try {
                t.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        heartbeatThread = null;
    }

    /**
     * Simulates processing one record per owned partition and committing the
     * new position. Fenced commits (stale generation / lost ownership /
     * replaced instance) are rejected by the coordinator and leave local
     * state untouched.
     */
    public void pollOnce() {
        int gen;
        long epoch;
        Set<Partition> owned;
        synchronized (lock) {
            if (fenced) {
                return;
            }
            gen = generation;
            epoch = memberEpoch;
            owned = assignment;
        }
        for (Partition p : owned) {
            long next;
            synchronized (lock) {
                next = positions.getOrDefault(p, -1L) + 1;
            }
            try {
                coordinator.commit(memberId, epoch, gen, p, next);
                synchronized (lock) {
                    positions.put(p, next);
                }
            } catch (CoordinatorException e) {
                commitRejections.incrementAndGet();
                switch (e.reason()) {
                    case STALE_GENERATION, UNKNOWN_MEMBER, ILLEGAL_GENERATION -> {
                        rejoin();
                        return;
                    }
                    case FENCED_MEMBER_EPOCH -> {
                        markFencedIfCurrent(epoch);
                        return;
                    }
                    default -> { /* NOT_OWNER / OFFSET_REGRESSION / IN_PROGRESS: retry later */ }
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

    /**
     * True once this instance was fenced by a newer instance of the same
     * member id. A fenced instance has stopped heartbeating and committing.
     */
    public boolean isFenced() {
        synchronized (lock) {
            return fenced;
        }
    }

    @Override
    public void close() {
        shutdown();
    }
}
