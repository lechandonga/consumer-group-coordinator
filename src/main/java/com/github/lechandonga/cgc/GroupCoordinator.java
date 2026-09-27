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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Central group coordinator: membership, heartbeats, generation-based fencing,
 * rebalance and validated offset commits.
 *
 * <p>Two usage styles are supported on top of one state machine:
 *
 * <ul>
 *   <li><b>Embedded single-process</b> ({@link #join(String)} /
 *       {@link #heartbeat(String,int)} / {@link #commit(String,int,Partition,long)}):
 *       membership changes are computed synchronously under the coordinator
 *       lock, so a new generation is established atomically.</li>
 *   <li><b>Multi-process protocol</b> ({@link #joinGroup(String,String)},
 *       {@link #syncGroup(String,String,int)} and the instance-token variants
 *       of heartbeat/commit): the rebalance is a two-phase window with a real,
 *       configured time bound ({@code rebalanceTimeoutMs}). While the window is
 *       open the group is {@link GroupState#REBALANCING} and callers observe
 *       {@link JoinStatus#IN_PROGRESS} / {@link RejectionReason#REBALANCE_IN_PROGRESS}
 *       — distinguishable from a final failure. When the bound elapses the
 *       rebalance closes deterministically: members that never rejoined are
 *       evicted and waiters receive a STABLE result flagged
 *       {@code completedByTimeout}.</li>
 * </ul>
 *
 * <p><b>Instance identity.</b> Every member registered through the multi-process
 * protocol receives an opaque instance token. All heartbeats/commits must carry
 * the current token; when the same member id restarts as a new (blank-token)
 * instance it is issued a fresh token and the previous instance is permanently
 * fenced ({@link RejectionReason#FENCED_INSTANCE}), even if it is still alive
 * in another process and keeps sending heartbeats or commits.
 *
 * <p>All public methods are thread-safe (serialized on the coordinator lock).
 */
public class GroupCoordinator implements AutoCloseable {

    /** Ownership interval used to audit that no partition ever has two live owners. */
    private record OwnershipInterval(Partition partition, String owner, int generation,
                                     long startMs, long endMs) {
    }

    private static final class MemberInfo {
        long lastHeartbeatMs;
        /** Current instance token; null means registered through the legacy embedded API. */
        String instanceToken;

        MemberInfo(long now, String instanceToken) {
            this.lastHeartbeatMs = now;
            this.instanceToken = instanceToken;
        }
    }

    /** Open two-phase rebalance window. */
    private static final class PendingRebalance {
        final int generation;
        final long deadlineMs;
        final Set<String> expected;
        final Set<String> rejoined = new LinkedHashSet<>();

        PendingRebalance(int generation, long deadlineMs, Set<String> expected) {
            this.generation = generation;
            this.deadlineMs = deadlineMs;
            this.expected = expected;
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
    private ScheduledFuture<?> pendingTimeoutTask;

    private GroupState state = GroupState.EMPTY;
    private int generation = 0;
    private PendingRebalance pending;
    /** Whether the most recently closed generation was closed by the time bound. */
    private boolean lastCompletedByTimeout;

    private static final AtomicLong INSTANCE_SEQ = new AtomicLong();

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
                    config.heartbeatIntervalMs(), config.heartbeatIntervalMs(),
                    TimeUnit.MILLISECONDS);
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

    // ------------------------------------------------------------------
    // Embedded single-process API (synchronous rebalance)
    // ------------------------------------------------------------------

    /**
     * Registers a member (or refreshes an existing one) and, for new members,
     * triggers a synchronous rebalance. Returns the current generation and the
     * member's assignment after the rebalance has completed.
     */
    public synchronized HeartbeatResponse join(String memberId) {
        // The embedded API never sees a pending window: close it immediately
        // and compute the generation synchronously (mixed API usage is not a
        // supported deployment; this only keeps the state machine total).
        if (pending != null) {
            cancelPendingTimeout();
            pending = null;
        }
        MemberInfo existing = members.get(memberId);
        if (existing != null) {
            existing.lastHeartbeatMs = clock.getAsLong();
        } else {
            members.put(memberId, new MemberInfo(clock.getAsLong(), null));
            rebalance();
        }
        return new HeartbeatResponse(generation, assignmentOf(memberId));
    }

    /** Renews the session; returns current generation and assignment. */
    public synchronized HeartbeatResponse heartbeat(String memberId, int gen)
            throws CoordinatorException {
        maybeTimeoutPending();
        MemberInfo member = requireMember(memberId);
        if (state == GroupState.REBALANCING) {
            throw new CoordinatorException(RejectionReason.REBALANCE_IN_PROGRESS,
                    "group is rebalancing");
        }
        checkGeneration(memberId, gen);
        member.lastHeartbeatMs = clock.getAsLong();
        return new HeartbeatResponse(this.generation, assignmentOf(memberId));
    }

    /** Voluntary leave; triggers a rebalance. */
    public synchronized void leave(String memberId) throws CoordinatorException {
        leaveInternal(memberId);
    }

    /** Commits an offset, fenced by generation and ownership. */
    public synchronized void commit(String memberId, int gen, Partition partition, long offset)
            throws CoordinatorException {
        commitInternal(memberId, null, gen, partition, offset, true);
    }

    // ------------------------------------------------------------------
    // Multi-process protocol: two-phase join, instance fencing
    // ------------------------------------------------------------------

    /**
     * Join phase 1.
     *
     * @param memberId member identity
     * @param token    blank/null means "I am a brand-new instance of this id"
     *                 (restart / takeover: the previous instance gets fenced);
     *                 otherwise it must match the member's current token
     * @return IN_PROGRESS while the rebalance window is open (call syncGroup),
     *         or STABLE for an idempotent re-sync of an already stable member
     */
    public synchronized JoinResult joinGroup(String memberId, String token)
            throws CoordinatorException {
        maybeTimeoutPending();
        long now = clock.getAsLong();
        MemberInfo existing = members.get(memberId);
        boolean takeover = token == null || token.isBlank();

        if (existing == null) {
            String issued = newToken();
            members.put(memberId, new MemberInfo(now, issued));
            openPending(now);
            pending.rejoined.add(memberId);
            return JoinResult.inProgress(pending.generation, issued);
        }

        if (takeover) {
            // Same member id, new instance: issue a fresh identity and fence
            // the old instance for good, even if it is still heartbeating.
            String issued = newToken();
            existing.instanceToken = issued;
            existing.lastHeartbeatMs = now;
            if (pending != null) {
                // The new instance satisfies this member's outstanding rejoin.
                pending.rejoined.add(memberId);
                tryCompletePending(false);
            } else {
                // Membership identity changed even though the id is the same:
                // the generation must advance so the old instance is fenced at
                // the generation level as well as the token level.
                openPending(now);
                pending.rejoined.add(memberId);
            }
            if (state == GroupState.REBALANCING) {
                return JoinResult.inProgress(pending.generation, issued);
            }
            return stableResult(memberId, issued);
        }

        // Existing member presenting its token: rejoin for the open window or
        // an idempotent re-sync when the group is already stable.
        checkInstance(existing, memberId, token);
        existing.lastHeartbeatMs = now;
        if (pending != null) {
            if (token.equals(existing.instanceToken)) {
                pending.rejoined.add(memberId);
            }
            if (pending.generation != generation) {
                throw new IllegalStateException("pending generation drift");
            }
            tryCompletePending(false);
            if (state == GroupState.REBALANCING) {
                return JoinResult.inProgress(pending.generation, token);
            }
            return stableResult(memberId, token);
        }
        if (generation == 0) {
            throw new CoordinatorException(RejectionReason.UNKNOWN_MEMBER,
                    "member '" + memberId + "' has no established generation");
        }
        return stableResult(memberId, token);
    }

    /**
     * Join phase 2: obtains the assignment once the generation closes. Returns
     * {@link JoinStatus#IN_PROGRESS} while the window remains open within its
     * configured bound; STABLE results carry {@code completedByTimeout} when
     * the bound elapsed before every expected member rejoined.
     */
    public synchronized JoinResult syncGroup(String memberId, String token, int gen)
            throws CoordinatorException {
        maybeTimeoutPending();
        MemberInfo member = requireMember(memberId);
        checkInstance(member, memberId, token);
        if (state == GroupState.REBALANCING) {
            if (gen != pending.generation) {
                if (gen < pending.generation) {
                    throw new CoordinatorException(RejectionReason.STALE_GENERATION,
                            "member '" + memberId + "' synced stale generation " + gen
                                    + ", current rebalance generation is " + pending.generation);
                }
                throw new CoordinatorException(RejectionReason.ILLEGAL_GENERATION,
                        "member '" + memberId + "' synced unknown generation " + gen);
            }
            member.lastHeartbeatMs = clock.getAsLong();
            pending.rejoined.add(memberId);
            tryCompletePending(false);
            if (state == GroupState.REBALANCING) {
                return JoinResult.inProgress(pending.generation, token);
            }
            return stableResult(memberId, token);
        }
        checkGeneration(memberId, gen);
        return stableResult(memberId, token);
    }

    /** Instance-fenced heartbeat. */
    public synchronized HeartbeatResponse heartbeat(String memberId, String token, int gen)
            throws CoordinatorException {
        maybeTimeoutPending();
        MemberInfo member = requireMember(memberId);
        checkInstance(member, memberId, token);
        if (state == GroupState.REBALANCING) {
            throw new CoordinatorException(RejectionReason.REBALANCE_IN_PROGRESS,
                    "group is rebalancing (generation " + pending.generation + " open)");
        }
        checkGeneration(memberId, gen);
        member.lastHeartbeatMs = clock.getAsLong();
        return new HeartbeatResponse(this.generation, assignmentOf(memberId));
    }

    /** Instance-fenced commit. */
    public synchronized void commit(String memberId, String token, int gen,
                                    Partition partition, long offset)
            throws CoordinatorException {
        commitInternal(memberId, token, gen, partition, offset, false);
    }

    /** Instance-fenced leave. */
    public synchronized void leave(String memberId, String token) throws CoordinatorException {
        MemberInfo member = members.get(memberId);
        if (member == null) {
            throw new CoordinatorException(RejectionReason.UNKNOWN_MEMBER,
                    "member '" + memberId + "' is not registered");
        }
        checkInstance(member, memberId, token);
        leaveInternal(memberId);
    }

    // ------------------------------------------------------------------
    // Shared internals
    // ------------------------------------------------------------------

    private void leaveInternal(String memberId) throws CoordinatorException {
        if (members.remove(memberId) == null) {
            throw new CoordinatorException(RejectionReason.UNKNOWN_MEMBER,
                    "member '" + memberId + "' is not registered");
        }
        if (pending != null) {
            pending.expected.remove(memberId);
            pending.rejoined.remove(memberId);
            tryCompletePending(false);
            if (pending != null) {
                return; // window stays open for the remaining members
            }
        } else {
            rebalance();
        }
    }

    private void commitInternal(String memberId, String token, int gen, Partition partition,
                                long offset, boolean embedded) throws CoordinatorException {
        maybeTimeoutPending();
        MemberInfo member = requireMember(memberId);
        if (!embedded) {
            checkInstance(member, memberId, token);
        }
        if (state == GroupState.REBALANCING) {
            throw new CoordinatorException(RejectionReason.REBALANCE_IN_PROGRESS,
                    "group is rebalancing; commit must be retried in the new generation");
        }
        checkGeneration(memberId, gen);
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

    /** Evicts members whose session expired; closes/rebalances if any were evicted. */
    public synchronized void checkExpirations() {
        maybeTimeoutPending();
        long now = clock.getAsLong();
        List<String> expired = new ArrayList<>();
        for (Map.Entry<String, MemberInfo> e : members.entrySet()) {
            if (now - e.getValue().lastHeartbeatMs > config.sessionTimeoutMs()) {
                expired.add(e.getKey());
            }
        }
        if (expired.isEmpty()) {
            return;
        }
        for (String id : expired) {
            members.remove(id);
        }
        if (pending != null) {
            pending.expected.removeAll(expired);
            pending.rejoined.removeAll(expired);
            tryCompletePending(false);
        } else {
            rebalance();
        }
    }

    private MemberInfo requireMember(String memberId) throws CoordinatorException {
        MemberInfo member = members.get(memberId);
        if (member == null) {
            throw new CoordinatorException(RejectionReason.UNKNOWN_MEMBER,
                    "member '" + memberId + "' is not registered");
        }
        return member;
    }

    /** Verifies that the presented token is the member's current instance identity. */
    private void checkInstance(MemberInfo member, String memberId, String token)
            throws CoordinatorException {
        if (member.instanceToken == null) {
            throw new CoordinatorException(RejectionReason.FENCED_INSTANCE,
                    "member '" + memberId + "' joined through the embedded API; "
                            + "instance-token requests are not accepted for it");
        }
        if (token == null || token.isBlank()) {
            throw new CoordinatorException(RejectionReason.FENCED_INSTANCE,
                    "member '" + memberId + "' presented no instance token");
        }
        if (!token.equals(member.instanceToken)) {
            throw new CoordinatorException(RejectionReason.FENCED_INSTANCE,
                    "instance of member '" + memberId + "' has been superseded; "
                            + "a newer instance now owns this identity");
        }
    }

    private void checkGeneration(String memberId, int gen) throws CoordinatorException {
        if (gen < this.generation) {
            throw new CoordinatorException(RejectionReason.STALE_GENERATION,
                    "member '" + memberId + "' used stale generation " + gen
                            + ", current is " + this.generation);
        }
        if (gen > this.generation) {
            throw new CoordinatorException(RejectionReason.ILLEGAL_GENERATION,
                    "member '" + memberId + "' used unknown generation " + gen
                            + ", current is " + this.generation);
        }
    }

    /** Opens a new two-phase rebalance window and fences the previous generation. */
    private void openPending(long now) {
        cancelPendingTimeout();
        generation++;
        state = GroupState.REBALANCING;
        // Fence every ownership of the previous generation before the window
        // opens; new ownership is installed only when the window closes.
        closeOwnershipIntervals(now);
        assignment = Map.of();
        ownership = Map.of();
        Set<String> expected = new LinkedHashSet<>(members.keySet());
        pending = new PendingRebalance(generation, now + config.rebalanceTimeoutMs(), expected);
        schedulePendingTimeout(config.rebalanceTimeoutMs());
    }

    /** Closes the window early if every expected member has rejoined. */
    private void tryCompletePending(boolean byTimeout) {
        if (pending == null) {
            return;
        }
        long now = clock.getAsLong();
        boolean timedOut = now >= pending.deadlineMs;
        if (!byTimeout && !timedOut && !pending.rejoined.containsAll(pending.expected)) {
            return;
        }
        finalizePending(timedOut, now);
    }

    /** Closes the window once the configured bound elapses, even with no waiter. */
    private void maybeTimeoutPending() {
        if (pending != null && clock.getAsLong() >= pending.deadlineMs) {
            finalizePending(true, clock.getAsLong());
        }
    }

    private void finalizePending(boolean byTimeout, long now) {
        PendingRebalance p = pending;
        cancelPendingTimeout();
        // Members that never rejoined are removed from the group.
        members.keySet().removeIf(id -> p.expected.contains(id) && !p.rejoined.contains(id));
        pending = null;
        lastCompletedByTimeout = byTimeout;
        installAssignment(now);
    }

    private void schedulePendingTimeout(long delayMs) {
        if (expiryScheduler == null) {
            return; // injected-clock tests drive time explicitly
        }
        long wait = Math.max(1, delayMs);
        pendingTimeoutTask = expiryScheduler.schedule(this::pendingTimeoutFired, wait,
                TimeUnit.MILLISECONDS);
    }

    private void pendingTimeoutFired() {
        synchronized (this) {
            try {
                maybeTimeoutPending();
            } catch (RuntimeException ignored) {
                // never kill the scheduler
            }
        }
    }

    private void cancelPendingTimeout() {
        if (pendingTimeoutTask != null) {
            pendingTimeoutTask.cancel(false);
            pendingTimeoutTask = null;
        }
    }

    private JoinResult stableResult(String memberId, String token) {
        return new JoinResult(JoinStatus.STABLE, generation, token, assignmentOf(memberId),
                lastCompletedByTimeout);
    }

    private static String newToken() {
        return "inst-" + Long.toHexString(INSTANCE_SEQ.incrementAndGet());
    }

    /** Synchronous rebalance used by the embedded API (generation established atomically). */
    private void rebalance() {
        cancelPendingTimeout();
        pending = null;
        long now = clock.getAsLong();
        generation++;
        state = GroupState.REBALANCING;
        closeOwnershipIntervals(now);
        installAssignment(now);
    }

    /** Installs the assignment for the current member set; state must be REBALANCING. */
    private void installAssignment(long now) {
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

    /** Deadline of the open rebalance window, or -1 when no window is open. */
    public synchronized long rebalanceDeadlineMs() {
        return pending == null ? -1L : pending.deadlineMs;
    }

    /** Ids the open window is still waiting for, or empty when no window is open. */
    public synchronized Set<String> pendingAwaiting() {
        return pending == null ? Set.of() : new LinkedHashSet<>(
                pending.expected.stream().filter(id -> !pending.rejoined.contains(id)).toList());
    }

    /** Whether the last closed generation was closed by the rebalance time bound. */
    public synchronized boolean lastCompletedByTimeout() {
        return lastCompletedByTimeout;
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
        cancelPendingTimeout();
        if (expiryScheduler != null) {
            expiryScheduler.shutdownNow();
        }
        store.close();
    }
}
