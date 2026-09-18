package com.github.lechandonga.cgcoordinator.audit;

/**
 * One ownership transition for a partition, used to detect illegal
 * overlapping ownership windows.
 */
public record OwnershipEvent(long epoch, long generation, int partition,
                             String oldOwner, String newOwner, long timestampMillis) {
}
