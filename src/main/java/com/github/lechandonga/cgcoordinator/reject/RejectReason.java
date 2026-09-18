package com.github.lechandonga.cgcoordinator.reject;

/**
 * Distinguishable rejection reasons for fencing/ownership/offset failures.
 * Every rejected request leaves established assignment state untouched.
 */
public enum RejectReason {
    UNKNOWN_MEMBER,
    STALE_GENERATION,
    FENCED_INCARNATION,
    REBALANCING,
    NOT_OWNER,
    REGRESSION,
    UNKNOWN_PARTITION,
    BAD_REQUEST
}
