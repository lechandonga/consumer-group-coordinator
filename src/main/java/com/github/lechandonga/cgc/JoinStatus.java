package com.github.lechandonga.cgc;

/**
 * Outcome status of a join/sync attempt under the two-phase join protocol.
 *
 * <p>{@link #IN_PROGRESS} is an observable, retryable intermediate state: the
 * member change was accepted and a new generation opened, but the rebalance
 * has not closed yet. It is deliberately distinct from a final failure
 * ({@link CoordinatorException}), which carries a {@link RejectionReason}.
 */
public enum JoinStatus {
    /** The rebalance has closed; the assignment carried in the result is valid. */
    STABLE,
    /** A rebalance is still open within its configured time bound; retry syncGroup. */
    IN_PROGRESS
}
