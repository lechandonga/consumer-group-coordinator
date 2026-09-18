package com.github.lechandonga.cgc;

import java.util.Map;
import java.util.Set;

/**
 * Simulated consumer process: heartbeats on a schedule, tracks its assignment,
 * commits processed offsets, and rejoins when fenced. Can be killed (simulated
 * crash) and restarted with the same id, resuming from committed offsets.
 */
public class ConsumerMember implements AutoCloseable {

    public ConsumerMember(String memberId, GroupCoordinator coordinator) {
        throw new UnsupportedOperationException("skeleton");
    }

    /** Starts the heartbeat loop. */
    public void start() {
        throw new UnsupportedOperationException("skeleton");
    }

    /** Simulates a crash: stops heartbeating without leaving the group. */
    public void kill() {
        throw new UnsupportedOperationException("skeleton");
    }

    /** Voluntary, clean shutdown (sends leave). */
    public void shutdown() {
        throw new UnsupportedOperationException("skeleton");
    }

    /** Simulates processing one record per owned partition and committing. */
    public void pollOnce() {
        throw new UnsupportedOperationException("skeleton");
    }

    public String memberId() {
        throw new UnsupportedOperationException("skeleton");
    }

    public Set<Partition> currentAssignment() {
        throw new UnsupportedOperationException("skeleton");
    }

    /** Local (in-memory) positions per partition, as this member sees them. */
    public Map<Partition, Long> localPositions() {
        throw new UnsupportedOperationException("skeleton");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("skeleton");
    }
}
