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
    /**
     * The member instance was replaced by a newer instance of the same member
     * id: the caller's member epoch is fenced and must stop, not retry.
     */
    FENCED_MEMBER_EPOCH,
    /** The member does not own the partition in the current generation. */
    NOT_OWNER,
    /** The committed offset would move the position backwards. */
    OFFSET_REGRESSION
}
