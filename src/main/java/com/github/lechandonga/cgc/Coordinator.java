package com.github.lechandonga.cgc;

/**
 * The coordination API a consumer member relies on. Implemented both by the
 * embedded {@link GroupCoordinator} (single-process) and by
 * {@code CoordinatorClient} (remote coordinator server over TCP), so the same
 * {@link ConsumerMember} logic runs unchanged in both deployment shapes.
 */
public interface Coordinator {

    CoordinatorConfig config();

    /**
     * Joins the group (or re-confirms an open rebalance barrier) and waits
     * until the group is stable; the returned response always carries the
     * final assignment. The wait is bounded by the coordinator's rebalance
     * deadline.
     */
    HeartbeatResponse joinAndAwait(String memberId) throws CoordinatorException, InterruptedException;

    /** Renews the session for this member instance. */
    HeartbeatResponse heartbeat(String memberId, long memberEpoch, int generation)
            throws CoordinatorException;

    /** Voluntary leave. */
    void leave(String memberId) throws CoordinatorException;

    /** Commits an offset, fenced by member epoch, generation and ownership. */
    void commit(String memberId, long memberEpoch, int generation, Partition partition, long offset)
            throws CoordinatorException;

    /** Last confirmed committed offset for a partition, or -1. */
    long fetchCommitted(Partition partition);
}
