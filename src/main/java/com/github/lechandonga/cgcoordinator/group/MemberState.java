package com.github.lechandonga.cgcoordinator.group;

/**
 * Server-side per-member record.
 *
 * @param memberId stable client-chosen identity (survives restart)
 * @param incarnationId unique per process start; fences zombie restarts
 * @param lastHeartbeatNanos last renewal time, System.nanoTime()
 * @param joinedGeneration generation in which the member joined
 */
public record MemberState(String memberId, String incarnationId,
                          long lastHeartbeatNanos, long joinedGeneration) {

    public MemberState withHeartbeat(long nowNanos) {
        return new MemberState(memberId, incarnationId, nowNanos, joinedGeneration);
    }
}
