package com.github.lechandonga.cgc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Central group coordinator: membership, heartbeats, generation-based fencing,
 * rebalance and validated offset commits.
 *
 * <p>State machine: {@code EMPTY -> REBALANCING -> STABLE}, and
 * {@code STABLE -> REBALANCING -> STABLE} on every membership change
 * (join / leave / expiry). Rebalance is computed synchronously under the
 * coordinator lock, so a new generation is established atomically: the moment
 * the generation advances, every ownership of the previous generation is
 * fenced and all further requests carrying it are rejected.
 *
 * <p>All public methods are thread-safe (serialized on the coordinator lock).
 */
public class GroupCoordinator implements AutoCloseable {

    /** Ownership interval used to audit that no partition ever has two live owners. */
    private record OwnershipInterval(Partition partition, String owner, int generation,
                                     long startMs, long endMs) {
    }

    private static final class MemberInfo {
        final long epoch;
        long lastHeartbeatMs;

        MemberInfo(long epoch, long now) {
            this.epoch = epoch;
            this.lastHeartbeatMs = now;
        }
    }

    private final CoordinatorConfig config;
    private final LongSupplier clock;
    private final OffsetCommitStore store;
    private final Assignor assignor = new RangeAssignor();
    private final List<Partition> partitions;

    private final Map<String, MemberInfo> members = new LinkedHashMap<>();
    private Map<String, Set<Partition>> assignment = Map.of();
    private Map<Partition, String> ownership = Map.of();
    private final List<OwnershipInterval> ownershipHistory = new ArrayList<>();

    private final ScheduledExecutorService expiryScheduler;

    private GroupState state = GroupState.EMPTY;
    private int generation = 0;
    private long nextMemberEpoch = 0;

    /** Production constructor: uses the wall clock and runs background expiry checks. */
    public GroupCoordinator(CoordinatorConfig config) {
        this(config, System::currentTimeMillis, true);
    }

    /** Test constructor with injectable clock (millis); expiry checks are manual. */
    public GroupCoordinator(CoordinatorConfig config, LongSupplier clock) {
        this(config, clock, false);
    }

    private GroupCoordinator(CoordinatorConfig config, LongSupplier clock, boolean autoExpiry) {
        this.config = config;
        this.clock = clock;
        this.store = new OffsetCommitStore(config.offsetStorePath());
        List<Partition> ps = new ArrayList<>();
        for (int i = 0; i < config.partitionCount(); i++) {
            ps.add(new Partition(config.topic(), i));
        }
        this.partitions = List.copyOf(ps);
        if (autoExpiry) {
            expiryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "group-coordinator-expiry");
                t.setDaemon(true);
                return t;
            });
            expiryScheduler.scheduleWithFixedDelay(this::checkExpirationsSafely,
                    config.heartbeatIntervalMs(), config.heartbeatIntervalMs(), TimeUnit.MILLISECONDS);
        } else {
            expiryScheduler = null;
        }
    }

    private void checkExpirationsSafely() {
        try {
            checkExpirations();
        } catch (RuntimeException ignored) {
            // Expiry checks must never kill the scheduler thread.
        }
    }

    public CoordinatorConfig config() {
        return config;
    }

    /**
     * Registers a member (or refreshes an existing one) and, for new members,
     * triggers a rebalance. Returns the current generation and the member's
     * assignment after any rebalance has completed.
     *
     * <p>Every join issues a fresh member epoch: if another instance of the
     * same member id is still live, it is fenced from this point on and its
     * further heartbeats/commits are rejected with
     * {@link RejectionReason#FENCED_MEMBER_EPOCH}. The joining instance
     * inherits the member id's current assignment without a rebalance, since
     * the membership set itself did not change.
     */
    public synchronized HeartbeatResponse join(String memberId) {
        MemberInfo existing = members.get(memberId);
        long epoch = ++nextMemberEpoch;
        if (existing != null) {
            members.put(memberId, new MemberInfo(epoch, clock.getAsLong()));
        } else {
            members.put(memberId, new MemberInfo(epoch, clock.getAsLong()));
            rebalance();
        }
        return new HeartbeatResponse(generation, epoch, assignmentOf(memberId), false);
    }

    /**
     * Legacy renew without an explicit member epoch: resolves to the member's
     * current epoch. Prefer {@link #heartbeat(String, long, int)} so that
     * replaced instances are fenced.
     */
    public synchronized HeartbeatResponse heartbeat(String memberId, int generation)
            throws CoordinatorException {
        MemberInfo member = members.get(memberId);
        if (member == null) {
            throw new CoordinatorException(RejectionReason.UNKNOWN_MEMBER,
                    "member '" + memberId + "' is not registered");
        }
        return heartbeat(memberId, member.epoch, generation);
    }

    /** Renews the session; returns current generation and assignment. */
    public synchronized HeartbeatResponse heartbeat(String memberId, long memberEpoch, int generation)
            throws CoordinatorException {
        MemberInfo member = members.get(memberId);
        if (member == null) {
            throw new CoordinatorException(RejectionReason.UNKNOWN_MEMBER,
                    "member '" + memberId + "' is not registered");
        }
        checkEpoch(memberId, member, memberEpoch);
        if (state == GroupState.REBALANCING) {
            throw new CoordinatorException(RejectionReason.REBALANCE_IN_PROGRESS,
                    "group is rebalancing");
        }
        checkGeneration(memberId, generation);
        member.lastHeartbeatMs = clock.getAsLong();
        return new HeartbeatResponse(this.generation, member.epoch, assignmentOf(memberId), false);
    }

    /** Voluntary leave; triggers a rebalance. */
    public synchronized void leave(String memberId) throws CoordinatorException {
        if (members.remove(memberId) == null) {
            throw new CoordinatorException(RejectionReason.UNKNOWN_MEMBER,
                    "member '" + memberId + "' is not registered");
        }
        rebalance();
    }

    /**
     * Legacy commit without an explicit member epoch: resolves to the member's
     * current epoch. Prefer {@link #commit(String, long, int, Partition, long)}
     * so that replaced instances are fenced.
     */
    public synchronized void commit(String memberId, int generation, Partition partition, long offset)
            throws CoordinatorException {
        MemberInfo member = members.get(memberId);
        if (member == null) {
            throw new CoordinatorException(RejectionReason.UNKNOWN_MEMBER,
                    "member '" + memberId + "' is not registered");
        }
        commit(memberId, member.epoch, generation, partition, offset);
    }

    /** Commits an offset, fenced by member epoch, generation and ownership. */
    public synchronized void commit(String memberId, long memberEpoch, int generation,
                                    Partition partition, long offset) throws CoordinatorException {
        MemberInfo member = members.get(memberId);
        if (member == null) {
            throw new CoordinatorException(RejectionReason.UNKNOWN_MEMBER,
                    "member '" + memberId + "' is not registered");
        }
        checkEpoch(memberId, member, memberEpoch);
        checkGeneration(memberId, generation);
        String owner = ownership.get(partition);
        if (!memberId.equals(owner)) {
            throw new CoordinatorException(RejectionReason.NOT_OWNER,
                    "member '" + memberId + "' does not own " + partition + " in generation "
                            + this.generation + " (owner: " + owner + ")");
        }
        long committed = store.committed(partition);
        if (offset < committed) {
            throw new CoordinatorException(RejectionReason.OFFSET_REGRESSION,
                    "offset " + offset + " for " + partition + " would regress committed " + committed);
        }
        store.commit(partition, offset);
    }

    /** Last confirmed committed offset for a partition, or -1. */
    public synchronized long fetchCommitted(Partition partition) {
        return store.committed(partition);
    }

    /** Evicts members whose session expired; triggers a rebalance if any were evicted. */
    public synchronized void checkExpirations() {
        long now = clock.getAsLong();
        List<String> expired = new ArrayList<>();
        for (Map.Entry<String, MemberInfo> e : members.entrySet()) {
            if (now - e.getValue().lastHeartbeatMs > config.sessionTimeoutMs()) {
                expired.add(e.getKey());
            }
        }
        if (!expired.isEmpty()) {
            for (String id : expired) {
                members.remove(id);
            }
            rebalance();
        }
    }

    private void checkEpoch(String memberId, MemberInfo member, long memberEpoch)
            throws CoordinatorException {
        if (memberEpoch != member.epoch) {
            throw new CoordinatorException(RejectionReason.FENCED_MEMBER_EPOCH,
                    "member '" + memberId + "' used fenced instance epoch " + memberEpoch
                            + ", current is " + member.epoch
                            + ": a newer instance of this member id has taken over");
        }
    }

    private void checkGeneration(String memberId, int generation) throws CoordinatorException {
        if (generation < this.generation) {
            throw new CoordinatorException(RejectionReason.STALE_GENERATION,
                    "member '" + memberId + "' used stale generation " + generation
                            + ", current is " + this.generation);
        }
        if (generation > this.generation) {
            throw new CoordinatorException(RejectionReason.ILLEGAL_GENERATION,
                    "member '" + memberId + "' used unknown generation " + generation
                            + ", current is " + this.generation);
        }
    }

    /** Establishes a new generation atomically. Caller must hold the lock. */
    private void rebalance() {
        state = GroupState.REBALANCING;
        long now = clock.getAsLong();
        generation++;
        // Fence every ownership of the previous generation before any new
        // ownership becomes valid: old intervals are closed first.
        closeOwnershipIntervals(now);
        if (members.isEmpty()) {
            assignment = Map.of();
            ownership = Map.of();
            state = GroupState.EMPTY;
            return;
        }
        assignment = assignor.assign(new ArrayList<>(members.keySet()), partitions);
        Map<Partition, String> newOwnership = new LinkedHashMap<>();
        for (Map.Entry<String, Set<Partition>> e : assignment.entrySet()) {
            for (Partition p : e.getValue()) {
                newOwnership.put(p, e.getKey());
            }
        }
        ownership = Collections.unmodifiableMap(newOwnership);
        for (Map.Entry<Partition, String> e : ownership.entrySet()) {
            ownershipHistory.add(
                    new OwnershipInterval(e.getKey(), e.getValue(), generation, now, -1L));
        }
        state = GroupState.STABLE;
    }

    private void closeOwnershipIntervals(long now) {
        for (int i = ownershipHistory.size() - 1; i >= 0; i--) {
            OwnershipInterval interval = ownershipHistory.get(i);
            if (interval.endMs() == -1L) {
                ownershipHistory.set(i, new OwnershipInterval(interval.partition(), interval.owner(),
                        interval.generation(), interval.startMs(), now));
            }
        }
    }

    public synchronized GroupState state() {
        return state;
    }

    public synchronized int generation() {
        return generation;
    }

    /** Current ownership map (partition -> member) for the stable generation. */
    public synchronized Map<Partition, String> ownershipSnapshot() {
        return ownership;
    }

    /** Assignment for a member in the current generation. */
    public synchronized Set<Partition> assignmentOf(String memberId) {
        return assignment.getOrDefault(memberId, Set.of());
    }

    /** Registered member ids. */
    public synchronized Set<String> members() {
        return new LinkedHashSet<>(members.keySet());
    }

    /** Current fencing epoch of a registered member, or -1 if not registered. */
    public synchronized long memberEpochOf(String memberId) {
        MemberInfo member = members.get(memberId);
        return member == null ? -1L : member.epoch;
    }

    /**
     * Verifies from the ownership history that no partition ever had two
     * simultaneously valid owners (intervals per partition must not overlap).
     */
    public synchronized boolean verifyNoOwnershipOverlap() {
        Map<Partition, List<OwnershipInterval>> byPartition = new HashMap<>();
        for (OwnershipInterval interval : ownershipHistory) {
            byPartition.computeIfAbsent(interval.partition(), k -> new ArrayList<>()).add(interval);
        }
        for (List<OwnershipInterval> intervals : byPartition.values()) {
            intervals.sort((a, b) -> Long.compare(a.startMs(), b.startMs()));
            for (int i = 1; i < intervals.size(); i++) {
                OwnershipInterval prev = intervals.get(i - 1);
                OwnershipInterval curr = intervals.get(i);
                long prevEnd = prev.endMs() == -1L ? Long.MAX_VALUE : prev.endMs();
                if (curr.startMs() < prevEnd) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public synchronized void close() {
        if (expiryScheduler != null) {
            expiryScheduler.shutdownNow();
        }
        store.close();
    }
}
