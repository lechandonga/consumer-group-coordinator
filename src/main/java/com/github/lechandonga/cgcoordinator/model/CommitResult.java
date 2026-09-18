package com.github.lechandonga.cgcoordinator.model;

/**
 * Result of an offset commit: success with the stored offset, or rejection.
 */
public record CommitResult(boolean accepted, String memberId, int partition,
                           long offset, long generation, String rejectReason) {

    public static CommitResult accepted(String memberId, int partition, long offset, long generation) {
        return new CommitResult(true, memberId, partition, offset, generation, null);
    }

    public static CommitResult rejected(String memberId, int partition, long offset,
                                        long generation, String reason) {
        return new CommitResult(false, memberId, partition, offset, generation, reason);
    }
}
