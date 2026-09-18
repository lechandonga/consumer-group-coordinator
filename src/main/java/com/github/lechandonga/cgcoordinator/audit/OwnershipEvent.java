package com.github.lechandonga.cgcoordinator.audit;

/**
 * One ownership transition for a partition.
 *
 * @param epoch           global monotonic event order
 * @param kind            GRANT or REVOKE
 * @param generation      group generation the transition belongs to
 * @param partition       partition id
 * @param oldOwner        owner before the transition (may be null)
 * @param newOwner        owner after the transition (may be null on final revoke)
 * @param timestampMillis event time
 */
public record OwnershipEvent(long epoch, OwnershipAuditLog.Kind kind, long generation,
                             int partition, String oldOwner, String newOwner,
                             long timestampMillis) {
}
