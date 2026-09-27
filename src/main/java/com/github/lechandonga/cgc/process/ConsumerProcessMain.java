package com.github.lechandonga.cgc.process;

import com.github.lechandonga.cgc.CoordinatorException;
import com.github.lechandonga.cgc.HeartbeatResponse;
import com.github.lechandonga.cgc.JoinResult;
import com.github.lechandonga.cgc.JoinStatus;
import com.github.lechandonga.cgc.Partition;
import com.github.lechandonga.cgc.RejectionReason;
import com.github.lechandonga.cgc.client.CoordinatorClient;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A real consumer process that joins a coordinator over TCP and performs the
 * full lifecycle against it:
 *
 * <ol>
 *   <li>phase-1 join (optionally as a fresh instance), phase-2 sync;</li>
 *   <li>background heartbeats at a fixed interval;</li>
 *   <li>consume/advance/commit loops on owned partitions;</li>
 *   <li>rejoin on generation changes; observe IN_PROGRESS as distinct from
 *       failure; stop deterministically when fenced as an old instance.</li>
 * </ol>
 *
 * <pre>
 *   java ... ConsumerProcessMain &lt;host&gt; &lt;port&gt; &lt;memberId&gt; [&lt;instanceToken&gt;|-]
 *        [--duration-ms N] [--slow-join] [--topic T] [--poll-ms N]
 * </pre>
 *
 * <p>Output (one event per line, flushed): {@code TOKEN ..}, {@code JOINED ..},
 * {@code ASSIGNED ..}, {@code COMMITTED ..}, {@code REBALANCING},
 * {@code TIMEOUT-CLOSED}, {@code REJECTED HEARTBEAT/COMMIT &lt;REASON&gt;},
 * {@code FENCED}. {@code --slow-join} performs phase 1 only and then idles,
 * letting the rebalance time bound elapse (used to exercise deterministic
 * timeout close end-to-end).
 */
public final class ConsumerProcessMain {

    private static final long HEARTBEAT_MS = 100;

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 0;
        String memberId = args.length > 2 ? args[2] : "consumer";
        String providedToken = args.length > 3 && !"-".equals(args[3]) ? args[3] : null;
        long durationMs = Long.MAX_VALUE;
        boolean slowJoin = false;
        boolean observeRebalance = false;
        String topic = "topic";
        long pollMs = 100;

        for (int i = 4; i < args.length; i++) {
            switch (args[i]) {
                case "--duration-ms" -> durationMs = Long.parseLong(args[++i]);
                case "--slow-join" -> slowJoin = true;
                case "--observe-rebalance" -> observeRebalance = true;
                case "--topic" -> topic = args[++i];
                case "--poll-ms" -> pollMs = Long.parseLong(args[++i]);
                default -> { /* ignore unknown flags for forward compatibility */ }
            }
        }

        int exitCode = run(host, port, memberId, providedToken, durationMs,
                slowJoin, observeRebalance, topic, pollMs);
        System.out.flush();
        System.exit(exitCode);
    }

    private static int run(String host, int port, String memberId, String providedToken,
                           long durationMs, boolean slowJoin, boolean observeRebalance,
                           String topic, long pollMs)
            throws Exception {
        try (CoordinatorClient client = new CoordinatorClient(host, port, topic)) {
            Session session = joinAndSync(client, memberId, providedToken, slowJoin);
            if (session == null) {
                return 1; // rejection already logged
            }
            session.observeRebalance = observeRebalance;
            if (slowJoin) {
                // Phase 1 done; deliberately never sync. Idle until duration
                // elapses so the coordinator's rebalance bound can fire.
                Thread.sleep(Math.min(durationMs, 30_000));
                return 0;
            }

            AtomicBoolean fenced = new AtomicBoolean(false);
            // Dedicated connection for heartbeats: a slow commit on the poll
            // connection must never head-of-line-block session renewal.
            final CoordinatorClient hbClient = new CoordinatorClient(host, port, topic);
            Thread heartbeater = new Thread(() ->
                    heartbeatLoop(hbClient, memberId, session, fenced),
                    "heartbeat-" + memberId);
            heartbeater.setDaemon(true);
            heartbeater.start();

            long deadline = System.currentTimeMillis() + durationMs;
            try {
                while (System.currentTimeMillis() < deadline && !fenced.get()) {
                    if (!consumeOnce(client, memberId, session, fenced)) {
                        break; // fenced
                    }
                    Thread.sleep(pollMs);
                }
            } finally {
                heartbeater.interrupt();
                heartbeater.join(2000);
                hbClient.close();
            }
            if (fenced.get()) {
                System.out.println("FENCED\t" + memberId);
                System.out.flush();
                return 3;
            }
            client.leave(memberId, session.token); // clean goodbye
            return 0;
        }
    }

    private static final class Session {
        final Object groupLock = new Object();
        volatile String token;
        volatile int generation;
        volatile Set<Partition> assignment = Set.of();
        final Map<Integer, Long> positions = new LinkedHashMap<>();
        /** Generation for which a rejoin is already in flight, so two threads
         *  (heartbeat + poll) do not each issue phase 1 for the same change. */
        int rejoinInProgressFor = Integer.MIN_VALUE;
        /** When true, REBALANCE_IN_PROGRESS is only observed/waited for; the
         *  member never issues phase 1 (used to exercise the timeout bound). */
        boolean observeRebalance;
    }

    /** Phase 1 + phase 2 with bounded retries while the window is IN_PROGRESS. */
    private static Session joinAndSync(CoordinatorClient client, String memberId,
                                       String providedToken, boolean slowJoin) throws Exception {
        // Outer retry: while several members join/change at almost the same
        // time, the window a member is confirming can be superseded or closed
        // under it. That is an in-progress situation, not a reason to die:
        // re-issue phase 1 and keep going. Only fencing is terminal.
        String nextToken = providedToken;
        for (long attempt = 0; attempt < 10_000; attempt++) {
            try {
                return joinAndSyncOnce(client, memberId, nextToken, slowJoin);
            } catch (CoordinatorException e) {
                System.out.println("REJECTED\tSYNC\t" + e.reason().name());
                System.out.flush();
                if (e.reason() == RejectionReason.FENCED_INSTANCE) {
                    return null; // permanently retired instance: stop, do not die
                }
                // Evicted instances re-enter as fresh instances; anything else
                // retries phase 1 carrying the current token.
                nextToken = e.reason() == RejectionReason.UNKNOWN_MEMBER ? null : nextToken;
                Thread.sleep(20);
            }
        }
        throw new IllegalStateException("join did not converge");
    }

    private static Session joinAndSyncOnce(CoordinatorClient client, String memberId,
                                           String providedToken, boolean slowJoin)
            throws Exception {
        JoinResult joined;
        try {
            joined = client.joinGroup(memberId, providedToken);
        } catch (CoordinatorException e) {
            System.out.println("REJECTED\tJOIN\t" + e.reason().name());
            System.out.flush();
            if (e.reason() == RejectionReason.FENCED_INSTANCE) {
                return null; // rejection already logged
            }
            throw e; // transient: the outer retry re-issues phase 1
        }
        System.out.println("TOKEN\t" + joined.instanceToken());
        System.out.flush();

        Session session = new Session();
        session.token = joined.instanceToken();
        session.generation = joined.generation();

        if (slowJoin) {
            System.out.println("JOINED\t" + memberId + "\t" + joined.generation() + "\tSLOW");
            System.out.flush();
            return session;
        }

        JoinResult synced = joined;
        long guard = 0;
        while (synced.status() == JoinStatus.IN_PROGRESS) {
            guard++;
            System.out.println("REBALANCING\t" + memberId + "\t" + synced.generation());
            System.out.flush();
            Thread.sleep(20);
            // Always confirm the newest generation the coordinator told us
            // about: while this member waits, a newer membership change can
            // supersede the window it had joined, and the IN_PROGRESS result
            // carries the generation that is open now.
            synced = client.syncGroup(memberId, session.token, synced.generation());
            if (guard > 100_000) {
                throw new IllegalStateException("sync loop did not terminate");
            }
        }
        applyAssignment(session, synced, client);
        System.out.println("JOINED\t" + memberId + "\t" + synced.generation()
                + (synced.completedByTimeout() ? "\tTIMEOUT-CLOSED" : ""));
        if (synced.completedByTimeout()) {
            System.out.println("TIMEOUT-CLOSED\t" + synced.generation());
        }
        System.out.println("ASSIGNED\t" + memberId + "\t" + format(synced.assignment()));
        System.out.flush();
        return session;
    }

    private static void applyAssignment(Session session, JoinResult result,
                                        CoordinatorClient client) {
        session.generation = result.generation();
        session.assignment = result.assignment();
        // Resume from last confirmed committed offsets.
        synchronized (session.positions) {
            session.positions.clear();
            for (Partition p : result.assignment()) {
                try {
                    session.positions.put(p.id(), client.fetchCommitted(p));
                } catch (Exception ignored) {
                    session.positions.put(p.id(), -1L);
                }
            }
        }
    }

    private static void heartbeatLoop(CoordinatorClient hbClient, String memberId,
                                      Session session, AtomicBoolean fenced) {
        while (!Thread.currentThread().isInterrupted() && !fenced.get()) {
            try {
                Thread.sleep(HEARTBEAT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            // Serialize group-state transitions against the poll loop, but
            // requests themselves run on this independent connection.
            synchronized (session.groupLock) {
                if (fenced.get()) {
                    return;
                }
                try {
                    HeartbeatResponse r = hbClient.heartbeat(memberId, session.token,
                            session.generation);
                    if (!r.assignment().equals(session.assignment)) {
                        session.assignment = r.assignment();
                        System.out.println("ASSIGNED\t" + memberId + "\t"
                                + format(r.assignment()));
                        System.out.flush();
                    }
                } catch (CoordinatorException e) {
                    if (handleCoordinatorFailure(e, "HEARTBEAT", hbClient, memberId,
                            session, fenced, false)) {
                        return;
                    }
                } catch (Exception e) {
                    return;
                }
            }
        }
    }

    /** Consumes one record per owned partition and commits the new position. */
    private static boolean consumeOnce(CoordinatorClient client, String memberId,
                                       Session session, AtomicBoolean fenced) {
        Set<Partition> owned;
        synchronized (session.groupLock) {
            owned = session.assignment;
        }
        for (Partition p : owned) {
            long next;
            synchronized (session.positions) {
                next = session.positions.getOrDefault(p.id(), -1L) + 1;
            }
            synchronized (session.groupLock) {
                if (fenced.get()) {
                    return false;
                }
                try {
                    client.commit(memberId, session.token, session.generation, p, next);
                    synchronized (session.positions) {
                        session.positions.put(p.id(), next);
                    }
                    System.out.println("COMMITTED\t" + memberId + "\tp" + p.id() + "\t" + next
                            + "\tgen" + session.generation);
                    System.out.flush();
                } catch (CoordinatorException e) {
                    if (handleCoordinatorFailure(e, "COMMIT", client, memberId,
                            session, fenced, !session.observeRebalance)) {
                        return false;
                    }
                } catch (Exception e) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Handles a rejection. Must be called while holding {@code session.groupLock}.
     *
     * @param mayRejoin whether a full phase-1 rejoin is allowed (commits yes; a
     *                  heartbeat merely follows an in-flight window and must not
     *                  open a new one, which would churn the generation)
     * @return true if this instance must stop (fenced / unrecoverable),
     *         false if it has caught up and may continue
     */
    private static boolean handleCoordinatorFailure(CoordinatorException e, String op,
                                                    CoordinatorClient client, String memberId,
                                                    Session session, AtomicBoolean fenced,
                                                    boolean mayRejoin) {
        RejectionReason reason = e.reason();
        System.out.println("REJECTED\t" + op + "\t" + reason.name());
        System.out.flush();
        if (reason == RejectionReason.FENCED_INSTANCE) {
            fenced.set(true);
            return true;
        }
        if (reason == RejectionReason.REBALANCE_IN_PROGRESS) {
            if (session.observeRebalance) {
                // Deliberately do not issue phase 1: just keep observing the
                // in-progress window until it closes at its time bound. Because
                // this member keeps heartbeating, the server retains it across
                // the timeout close and reports TIMEOUT-CLOSED.
                try {
                    followCurrentLocked(client, memberId, session);
                } catch (CoordinatorException ce) {
                    if (ce.reason() == RejectionReason.FENCED_INSTANCE) {
                        fenced.set(true);
                        return true;
                    }
                } catch (Exception ignored) {
                    // retry next tick
                }
                return false;
            }
            // Normal member: participates in the open generation (phase 1 with
            // the current token never opens a new window) and waits to close.
            try {
                rejoinLocked(client, memberId, session, false);
            } catch (CoordinatorException ce) {
                if (ce.reason() == RejectionReason.FENCED_INSTANCE) {
                    fenced.set(true);
                    System.out.println("REJECTED\tREJOIN\tFENCED_INSTANCE");
                    System.out.flush();
                    return true;
                }
            } catch (Exception ignored) {
                // transport: retry next tick
            }
            return false;
        }
        if (reason == RejectionReason.STALE_GENERATION
                || reason == RejectionReason.ILLEGAL_GENERATION
                || reason == RejectionReason.UNKNOWN_MEMBER) {
            // The group moved on without us (or we were evicted). Only the
            // poll loop issues phase 1, so the heartbeat and the poll loop can
            // never open competing windows for the same membership change.
            if (mayRejoin) {
                try {
                    // UNKNOWN (evicted, not superseded): re-enter as a new
                    // instance; STALE: rejoin carrying the current token.
                    boolean asNew = reason == RejectionReason.UNKNOWN_MEMBER;
                    rejoinLocked(client, memberId, session, asNew);
                } catch (CoordinatorException ce) {
                    if (ce.reason() == RejectionReason.FENCED_INSTANCE) {
                        fenced.set(true);
                        System.out.println("REJECTED\tREJOIN\tFENCED_INSTANCE");
                        System.out.flush();
                        return true;
                    }
                } catch (Exception ignored) {
                    // transport: retry next tick
                }
            } else {
                // Heartbeat path: the change has already closed and we are
                // behind/evicted. The group lock serializes this against the
                // poll loop, so it is safe to perform the single rejoin here.
                try {
                    boolean asNew = reason == RejectionReason.UNKNOWN_MEMBER;
                    rejoinLocked(client, memberId, session, asNew);
                } catch (CoordinatorException ce) {
                    if (ce.reason() == RejectionReason.FENCED_INSTANCE) {
                        fenced.set(true);
                        return true;
                    }
                } catch (Exception ignored) {
                    // retry next heartbeat
                }
            }
            return false;
        }
        // NOT_OWNER / OFFSET_REGRESSION: transient for this loop iteration.
        return false;
    }

    /** Waits for and adopts the currently open generation without a new join. */
    private static void followCurrentLocked(CoordinatorClient client, String memberId,
                                            Session session) throws Exception {
        JoinResult synced = client.syncCurrent(memberId, session.token);
        long guard = 0;
        while (synced.status() == JoinStatus.IN_PROGRESS) {
            if (guard++ > 100_000) {
                throw new IllegalStateException("follow-current loop did not terminate");
            }
            Thread.sleep(10);
            synced = client.syncCurrent(memberId, session.token);
        }
        session.token = synced.instanceToken();
        applyAssignment(session, synced, client);
        System.out.println("ASSIGNED\t" + memberId + "\t" + format(synced.assignment())
                + (synced.completedByTimeout() ? "\tTIMEOUT-CLOSED" : ""));
        System.out.flush();
    }

    /**
     * Full phase-1 + phase-2 rejoin. Caller holds {@code session.groupLock}.
     * When {@code asNewInstance} is true the member rejoins with a blank token
     * (fresh identity); otherwise it presents its current instance token.
     */
    private static void rejoinLocked(CoordinatorClient client, String memberId, Session session,
                                     boolean asNewInstance)
            throws Exception {
        JoinResult joined = client.joinGroup(memberId,
                asNewInstance ? null : session.token);
        session.token = joined.instanceToken();
        JoinResult synced = joined;
        long guard = 0;
        while (synced.status() == JoinStatus.IN_PROGRESS) {
            if (guard++ > 100_000) {
                throw new IllegalStateException("rejoin sync loop did not terminate");
            }
            Thread.sleep(10);
            // Confirm the newest generation reported by the coordinator: the
            // window may have been superseded by a concurrent membership
            // change while this member was waiting for it to close.
            synced = client.syncGroup(memberId, session.token, synced.generation());
        }
        session.token = synced.instanceToken();
        applyAssignment(session, synced, client);
        System.out.println("ASSIGNED\t" + memberId + "\t" + format(synced.assignment())
                + (synced.completedByTimeout() ? "\tTIMEOUT-CLOSED" : ""));
        System.out.flush();
    }

    private static String format(Set<Partition> partitions) {
        StringBuilder sb = new StringBuilder();
        partitions.stream().map(Partition::id).sorted().forEach(id -> {
            if (!sb.isEmpty()) {
                sb.append(",");
            }
            sb.append("p").append(id);
        });
        return sb.toString();
    }

    private ConsumerProcessMain() {
    }
}
