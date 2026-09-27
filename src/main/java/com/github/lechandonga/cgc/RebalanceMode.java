package com.github.lechandonga.cgc;

/**
 * How the coordinator finalizes a membership change.
 */
public enum RebalanceMode {
    /**
     * Legacy embedded behavior: the rebalance is computed atomically under the
     * coordinator lock, so a new generation (fencing + assignment) is
     * established in the same instant the membership change is applied.
     * No in-progress window is ever observable.
     */
    IMMEDIATE,
    /**
     * Barrier-based rebalance with a real time bound: a membership change
     * fences the old generation immediately and opens a confirmation barrier
     * bounded by {@code rebalanceTimeoutMs}. The new generation's assignment
     * is established once every live member has re-confirmed, or -
     * deterministically - when the deadline elapses, evicting the members
     * that failed to confirm. While the barrier is open, generation-carrying
     * requests are rejected with {@link RejectionReason#REBALANCE_IN_PROGRESS},
     * a result distinguishable from terminal failures.
     */
    BARRIER
}
