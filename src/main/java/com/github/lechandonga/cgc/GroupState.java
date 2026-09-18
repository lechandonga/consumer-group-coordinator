package com.github.lechandonga.cgc;

/** Coordinator group state machine states. */
public enum GroupState {
    /** No members. */
    EMPTY,
    /** Membership changed; a new generation is being computed. */
    REBALANCING,
    /** A valid generation with a complete, non-overlapping assignment exists. */
    STABLE
}
