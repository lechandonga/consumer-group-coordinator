package com.github.lechandonga.cgc;

/** Distinguishable reasons for a rejected coordinator request. */
public enum RejectionReason {
    /** Member id is not registered (never joined or already expired/left). */
    UNKNOWN_MEMBER,
    /** Caller supplied a generation older than the current group generation. */
    STALE_GENERATION,
    /** Caller supplied a generation newer than any generation the group has reached. */
    ILLEGAL_GENERATION,
    /** A rebalance is being computed; requests must be retried afterwards. */
    REBALANCE_IN_PROGRESS,
    /** The member does not own the partition in the current generation. */
    NOT_OWNER,
    /** The committed offset would move the position backwards. */
    OFFSET_REGRESSION,
    /**
     * The caller's instance identity (instance token) is no longer current for
     * its member id: a newer instance has taken over the identity, or this
     * instance has been evicted. The request is fenced and must not be retried
     * by this instance.
     */
    FENCED_INSTANCE
}
