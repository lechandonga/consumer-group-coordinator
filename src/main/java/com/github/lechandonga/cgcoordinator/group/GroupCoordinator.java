package com.github.lechandonga.cgcoordinator.group;

import com.github.lechandonga.cgcoordinator.assign.PartitionAssignor;
import com.github.lechandonga.cgcoordinator.audit.OwnershipAuditLog;
import com.github.lechandonga.cgcoordinator.config.CoordinatorConfig;
import com.github.lechandonga.cgcoordinator.model.Assignment;
import com.github.lechandonga.cgcoordinator.model.CommitResult;
import com.github.lechandonga.cgcoordinator.model.Snapshot;
import com.github.lechandonga.cgcoordinator.offset.OffsetStore;
import com.github.lechandonga.cgcoordinator.reject.CoordinatorException;
import com.github.lechandonga.cgcoordinator.reject.RejectReason;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory, single-monitor group coordination core.
 *
 * <h2>State machine</h2>
 * <pre>
 *   EMPTY --first join--> REBALANCING --all members joined--> STABLE
 *   STABLE --join/leave/expiry--> REBALANCING --rejoin--> STABLE
 * </pre>
 *
 * <h2>Fencing</h2>
 * Every request carries {@code (memberId, incarnationId, generation)}.
 * A restart creates a new incarnation id: the zombie incarnation is fenced
 * ({@link RejectReason#FENCED_INCARNATION}); requests using a generation
 * other than the current one are rejected with
 * {@link RejectReason#STALE_GENERATION}; requests during REBALANCING are
 * rejected with {@link RejectReason#REBALANCING}. Rejected requests never
 * mutate established ownership.
 *
 * <h2>Single ownership invariant</h2>
 * Every state transition runs under one lock and performs REVOKE of the old
 * generation for every partition before GRANT in the new generation, so at
 * no instant do two established assignments overlap. The
 * {@link OwnershipAuditLog} verifies this property.
 */
public class GroupCoordinator {

    private final CoordinatorConfig config;
    private final PartitionAssignor assignor;
    private final OffsetStore offsets;
    private final OwnershipAuditLog audit;

    private GroupState state = GroupState.EMPTY;
    private long currentGeneration = 0;
    private final Map<String, MemberState> members = new LinkedHashMap<>();
    private final Map<Integer, String> ownerByPartition = new HashMap<>();
    private final Map<String, List<Integer>> assignmentByMember = new LinkedHashMap<>();

    // Rebalance bookkeeping (only meaningful in REBALANCING).
    private final Map<String, String> joined = new LinkedHashMap<>();
    private final java.util.Set<String> expected = new java.util.LinkedHashSet<>();
    private long rebalanceDeadlineNanos = Long.MAX_VALUE;

    public GroupCoordinator(CoordinatorConfig config, PartitionAssignor assignor,
                            OffsetStore offsets, OwnershipAuditLog audit) {
        this.config = config;
        this.assignor = assignor;
        this.offsets = offsets;
        this.audit = audit;
    }

    // ------------------------------------------------------------------ join

    /**
     * Register (or re-register after restart) and block until the triggered
     * rebalance completes, then return the member's assignment in the new
     * generation. If the same member is already present with an older
     * incarnation it is replaced and the zombie incarnation is fenced.
     */
    public synchronized Assignment joinGroup(String memberId, String incarnationId,
                                             long timeoutMillis) {
        if (memberId == null || memberId.isBlank()
                || incarnationId == null || incarnationId.isBlank()) {
            throw new CoordinatorException(RejectReason.BAD_REQUEST,
                    "memberId and incarnationId must not be blank");
        }
        // beginRebalance is a no-op if a rebalance is already collecting.
        beginRebalanceNano(System.nanoTime());
        upsertJoiningMember(memberId, incarnationId);
        waitForRebalance();
        if (state == GroupState.REBALANCING) {
            throw new CoordinatorException(RejectReason.REBALANCING,
                    "join timed out for member " + memberId);
        }
        if (!joined.containsKey(memberId)) {
            // Member was finalized out of this generation (slow join); it must
            // re-join rather than receive a half-established assignment.
            throw new CoordinatorException(RejectReason.REBALANCING,
                    "member " + memberId + " missed the join window; re-join required");
        }
        List<Integer> parts = assignmentByMember.getOrDefault(memberId, List.of());
        return new Assignment(currentGeneration, memberId, parts);
    }

    private void upsertJoiningMember(String memberId, String incarnationId) {
        long now = System.nanoTime();
        members.put(memberId, new MemberState(memberId, incarnationId,
                now, currentGeneration));
        // The joining member is part of the expected set for early completion.
        expected.add(memberId);
        joined.put(memberId, incarnationId);
        if (joined.keySet().equals(expected)) {
            // Every pre-rebalance member (plus newcomers so far) has joined:
            // complete immediately, never waiting out the whole window.
            completeRebalanceNano(now);
        }
    }

    /**
     * Waits until the current rebalance completes. Completion is guaranteed by
     * the fixed join window: the first waiting thread that observes the
     * deadline finalizes the generation (removing members that never joined).
     */
    private void waitForRebalance() {
        while (state == GroupState.REBALANCING) {
            long remaining = rebalanceDeadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                finalizeRebalanceWithLiveMembersNano(System.nanoTime());
                break;
            }
            long waitMs = Math.min(remaining / 1_000_000L, config.joinTimeoutMillis());
            try {
                wait(Math.max(waitMs, 1L));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CoordinatorException(RejectReason.BAD_REQUEST,
                        "join interrupted");
            }
            if (state == GroupState.REBALANCING
                    && System.nanoTime() >= rebalanceDeadlineNanos) {
                finalizeRebalanceWithLiveMembersNano(System.nanoTime());
            }
        }
    }

    private void beginRebalanceNano(long nowNanos) {
        if (state == GroupState.REBALANCING) {
            return;
        }
        state = GroupState.REBALANCING;
        long nextGeneration = currentGeneration + 1;
        // Revoke every partition of the outgoing generation atomically first.
        for (Map.Entry<Integer, String> e : ownerByPartition.entrySet()) {
            audit.revoke(currentGeneration, e.getKey(), e.getValue(), null,
                    nowNanos / 1_000_000L);
        }
        ownerByPartition.clear();
        assignmentByMember.clear();
        joined.clear();
        expected.clear();
        expected.addAll(members.keySet());
        currentGeneration = nextGeneration;
        rebalanceDeadlineNanos = nowNanos + config.joinTimeoutMillis() * 1_000_000L;
    }

    private void completeRebalanceNano(long nowNanos) {
        Map<String, List<Integer>> plan =
                assignor.assign(new ArrayList<>(members.keySet()), config.partitionCount());
        applyPlan(plan, nowNanos);
        state = GroupState.STABLE;
        rebalanceDeadlineNanos = Long.MAX_VALUE;
        notifyAll();
    }

    private void finalizeRebalanceWithLiveMembersNano(long nowNanos) {
        if (state != GroupState.REBALANCING) {
            return;
        }
        // The fixed join window elapsed: members that did not re-join are out
        // of this generation.
        members.keySet().retainAll(joined.keySet());
        if (members.isEmpty()) {
            state = GroupState.EMPTY;
            joined.clear();
            expected.clear();
            assignmentByMember.clear();
            ownerByPartition.clear();
            rebalanceDeadlineNanos = Long.MAX_VALUE;
            notifyAll();
            return;
        }
        completeRebalanceNano(nowNanos);
    }

    private void applyPlan(Map<String, List<Integer>> plan, long nowNanos) {
        Map<Integer, String> nextOwners = new HashMap<>();
        Map<String, List<Integer>> nextAssignment = new LinkedHashMap<>();
        int seen = 0;
        for (Map.Entry<String, List<Integer>> e : plan.entrySet()) {
            String member = e.getKey();
            List<Integer> parts = List.copyOf(e.getValue());
            nextAssignment.put(member, parts);
            for (int p : parts) {
                String previous = nextOwners.put(p, member);
                if (previous != null) {
                    throw new IllegalStateException(
                            "assignor produced overlapping ownership for partition " + p);
                }
                seen++;
            }
        }
        if (seen != config.partitionCount()) {
            throw new IllegalStateException(
                    "assignor covered " + seen + " partitions but expected "
                            + config.partitionCount());
        }
        // Commit to the new ownership table and emit GRANT events only after
        // the old generation was fully revoked: no overlap window.
        ownerByPartition.clear();
        ownerByPartition.putAll(nextOwners);
        assignmentByMember.clear();
        assignmentByMember.putAll(nextAssignment);
        long ts = nowNanos / 1_000_000L;
        for (Map.Entry<Integer, String> e : ownerByPartition.entrySet()) {
            audit.grant(currentGeneration, e.getKey(), null, e.getValue(), ts);
        }
        for (MemberState m : members.values()) {
            String inc = joined.get(m.memberId());
            members.put(m.memberId(),
                    new MemberState(m.memberId(),
                            inc != null ? inc : m.incarnationId(),
                            nowNanos, currentGeneration));
        }
    }

    // ------------------------------------------------------------- heartbeat

    public synchronized void heartbeat(String memberId, String incarnationId,
                                       long generation) {
        MemberState m = requireKnown(memberId, incarnationId);
        if (generation != currentGeneration) {
            throw new CoordinatorException(RejectReason.STALE_GENERATION,
                    "heartbeat generation " + generation + " != current " + currentGeneration);
        }
        if (state == GroupState.REBALANCING) {
            // Heartbeat itself is fine, but the member must re-join; it may
            // not assume ownership until STABLE.
            members.put(memberId, m.withHeartbeat(System.nanoTime()));
            throw new CoordinatorException(RejectReason.REBALANCING,
                    "group is rebalancing; member must re-join");
        }
        members.put(memberId, m.withHeartbeat(System.nanoTime()));
    }

    // ----------------------------------------------------------------- leave

    public synchronized void leave(String memberId, String incarnationId, long generation) {
        if (!members.containsKey(memberId)) {
            throw new CoordinatorException(RejectReason.UNKNOWN_MEMBER,
                    "unknown member " + memberId);
        }
        requireKnown(memberId, incarnationId);
        if (generation != currentGeneration) {
            throw new CoordinatorException(RejectReason.STALE_GENERATION,
                    "leave generation " + generation + " != current " + currentGeneration);
        }
        members.remove(memberId);
        if (members.isEmpty()) {
            revokeAllNano(System.nanoTime());
            state = GroupState.EMPTY;
            currentGeneration++;
            assignmentByMember.clear();
            notifyAll();
        } else if (state == GroupState.STABLE) {
            beginRebalanceNano(System.nanoTime());
            // The leaving member is already absent; allow live members to
            // complete the rebalance when they re-join.
        }
    }

    private void revokeAllNano(long nowNanos) {
        long ts = nowNanos / 1_000_000L;
        for (Map.Entry<Integer, String> e : ownerByPartition.entrySet()) {
            audit.revoke(currentGeneration, e.getKey(), e.getValue(), null, ts);
        }
        ownerByPartition.clear();
        assignmentByMember.clear();
        joined.clear();
    }

    // ---------------------------------------------------------------- commit

    public synchronized CommitResult commit(String memberId, String incarnationId,
                                            long generation, int partition, long offset) {
        try {
            if (partition < 0 || partition >= config.partitionCount()) {
                throw new CoordinatorException(RejectReason.UNKNOWN_PARTITION,
                        "partition " + partition + " out of range [0,"
                                + config.partitionCount() + ")");
            }
            requireKnown(memberId, incarnationId);
            if (generation != currentGeneration) {
                throw new CoordinatorException(RejectReason.STALE_GENERATION,
                        "commit generation " + generation + " != current " + currentGeneration);
            }
            if (state != GroupState.STABLE) {
                throw new CoordinatorException(RejectReason.REBALANCING,
                        "cannot commit while rebalancing");
            }
            String owner = ownerByPartition.get(partition);
            if (owner == null || !owner.equals(memberId)) {
                throw new CoordinatorException(RejectReason.NOT_OWNER,
                        "member " + memberId + " does not own partition " + partition
                                + " (owner=" + owner + ")");
            }
            offsets.save(memberId, partition, offset, generation);
            return CommitResult.accepted(memberId, partition, offset, generation);
        } catch (CoordinatorException e) {
            return CommitResult.rejected(memberId, partition, offset, generation,
                    e.reason().name());
        }
    }

    // ----------------------------------------------------------------- fetch

    public synchronized long fetchOffset(int partition) {
        if (partition < 0 || partition >= config.partitionCount()) {
            throw new CoordinatorException(RejectReason.UNKNOWN_PARTITION,
                    "partition " + partition + " out of range");
        }
        return offsets.fetch(partition);
    }

    // -------------------------------------------------------------- sessions

    /**
     * Expire members whose last heartbeat is older than the session timeout.
     * Returns true if expiry triggered (or completed) a rebalance. Safe to
     * call periodically from a reaper thread.
     */
    public synchronized boolean expireSessions(long nowNanos) {
        boolean changed = false;
        List<String> dead = new ArrayList<>();
        for (MemberState m : members.values()) {
            long age = nowNanos - m.lastHeartbeatNanos();
            if (age > config.sessionTimeoutMillis() * 1_000_000L) {
                dead.add(m.memberId());
            }
        }
        for (String m : dead) {
            members.remove(m);
            joined.remove(m);
            changed = true;
        }
        if (!changed) {
            return false;
        }
        if (members.isEmpty()) {
            revokeAllNano(nowNanos);
            state = GroupState.EMPTY;
            currentGeneration++;
            notifyAll();
            return true;
        }
        if (state == GroupState.STABLE) {
            beginRebalanceNano(nowNanos);
            // Remaining members must re-join within the window.
        }
        if (state == GroupState.REBALANCING) {
            // Removal may let the still-joining generation complete early.
            expected.retainAll(members.keySet());
            if (!expected.isEmpty() && joined.keySet().equals(expected)) {
                completeRebalanceNano(nowNanos);
            }
        }
        return true;
    }

    /** Drives join deadlines; invoked by a periodic reaper. */
    public synchronized boolean tickRebalance(long nowNanos) {
        if (state == GroupState.REBALANCING && nowNanos >= rebalanceDeadlineNanos) {
            finalizeRebalanceWithLiveMembersNano(nowNanos);
            return true;
        }
        return false;
    }

    // -------------------------------------------------------------- helpers

    private MemberState requireKnown(String memberId, String incarnationId) {
        MemberState m = members.get(memberId);
        if (m == null) {
            throw new CoordinatorException(RejectReason.UNKNOWN_MEMBER,
                    "unknown member " + memberId);
        }
        if (!m.incarnationId().equals(incarnationId)) {
            throw new CoordinatorException(RejectReason.FENCED_INCARNATION,
                    "incarnation " + incarnationId + " for member " + memberId
                            + " is fenced; current incarnation " + m.incarnationId());
        }
        return m;
    }

    private static long millis(long timeoutMillis) {
        return timeoutMillis <= 0 ? 1L : timeoutMillis;
    }

    // --------------------------------------------------------------- views

    public synchronized Snapshot snapshot() {
        Map<String, List<Integer>> copy = new LinkedHashMap<>();
        assignmentByMember.forEach((k, v) -> copy.put(k, List.copyOf(v)));
        return new Snapshot(currentGeneration, List.copyOf(members.keySet()),
                copy, state.name());
    }

    public synchronized GroupState state() {
        return state;
    }

    public synchronized long generation() {
        return currentGeneration;
    }

    /** Established owner of a partition, or empty. */
    public synchronized List<String> ownersOf(int partition) {
        String owner = ownerByPartition.get(partition);
        return owner == null ? List.of() : List.of(owner);
    }

    public synchronized boolean assignmentCoversAndDisjoint() {
        int count = 0;
        boolean[] seen = new boolean[config.partitionCount()];
        for (List<Integer> parts : assignmentByMember.values()) {
            for (int p : parts) {
                if (p < 0 || p >= config.partitionCount() || seen[p]) {
                    return false;
                }
                seen[p] = true;
                count++;
            }
        }
        return count == config.partitionCount();
    }
}
