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
        String topic = "topic";
        long pollMs = 100;

        for (int i = 4; i < args.length; i++) {
            switch (args[i]) {
                case "--duration-ms" -> durationMs = Long.parseLong(args[++i]);
                case "--slow-join" -> slowJoin = true;
                case "--topic" -> topic = args[++i];
                case "--poll-ms" -> pollMs = Long.parseLong(args[++i]);
                default -> { /* ignore unknown flags for forward compatibility */ }
            }
        }

        int exitCode = run(host, port, memberId, providedToken, durationMs,
                slowJoin, topic, pollMs);
        System.out.flush();
        System.exit(exitCode);
    }

    private static int run(String host, int port, String memberId, String providedToken,
                           long durationMs, boolean slowJoin, String topic, long pollMs)
            throws Exception {
        try (CoordinatorClient client = new CoordinatorClient(host, port, topic)) {
            Session session = joinAndSync(client, memberId, providedToken, slowJoin);
            if (session == null) {
                return 1; // rejection already logged
            }
            if (slowJoin) {
                // Phase 1 done; deliberately never sync. Idle until duration
                // elapses so the coordinator's rebalance bound can fire.
                Thread.sleep(Math.min(durationMs, 30_000));
                return 0;
            }

            AtomicBoolean fenced = new AtomicBoolean(false);
            Thread heartbeater = new Thread(() ->
                    heartbeatLoop(client, memberId, topic, session, fenced),
                    "heartbeat-" + memberId);
            heartbeater.setDaemon(true);
            heartbeater.start();

            long deadline = System.currentTimeMillis() + durationMs;
            while (System.currentTimeMillis() < deadline && !fenced.get()) {
                if (!consumeOnce(client, memberId, topic, session, fenced)) {
                    break; // fenced
                }
                Thread.sleep(pollMs);
            }

            heartbeater.interrupt();
            heartbeater.join(2000);
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
        volatile String token;
        volatile int generation;
        volatile Set<Partition> assignment = Set.of();
        final Map<Integer, Long> positions = new LinkedHashMap<>();
    }

    /** Phase 1 + phase 2 with bounded retries while the window is IN_PROGRESS. */
    private static Session joinAndSync(CoordinatorClient client, String memberId,
                                       String providedToken, boolean slowJoin) throws Exception {
        JoinResult joined;
        try {
            joined = client.joinGroup(memberId, providedToken);
        } catch (CoordinatorException e) {
            System.out.println("REJECTED\tJOIN\t" + e.reason().name());
            System.out.flush();
            return null;
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
            System.out.println("REBALANCING\t" + memberId + "\t" + joined.generation());
            System.out.flush();
            Thread.sleep(20);
            synced = client.syncGroup(memberId, session.token, joined.generation());
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

    private static void heartbeatLoop(CoordinatorClient client, String memberId, String topic,
                                      Session session, AtomicBoolean fenced) {
        while (!Thread.currentThread().isInterrupted() && !fenced.get()) {
            try {
                Thread.sleep(HEARTBEAT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                HeartbeatResponse r = client.heartbeat(memberId, session.token, session.generation);
                if (!r.assignment().equals(session.assignment)) {
                    session.assignment = r.assignment();
                    System.out.println("ASSIGNED\t" + memberId + "\t" + format(r.assignment()));
                    System.out.flush();
                }
            } catch (CoordinatorException e) {
                if (handleCoordinatorFailure(e, "HEARTBEAT", client, memberId, topic,
                        session, fenced)) {
                    return;
                }
            } catch (Exception e) {
                return;
            }
        }
    }

    /** Consumes one record per owned partition and commits the new position. */
    private static boolean consumeOnce(CoordinatorClient client, String memberId, String topic,
                                       Session session, AtomicBoolean fenced) {
        Set<Partition> owned = session.assignment;
        for (Partition p : owned) {
            long next;
            synchronized (session.positions) {
                next = session.positions.getOrDefault(p.id(), -1L) + 1;
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
                if (handleCoordinatorFailure(e, "COMMIT", client, memberId, topic,
                        session, fenced)) {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return true if this instance must stop (fenced / unrecoverable),
     *         false if it has rejoined and may continue
     */
    private static boolean handleCoordinatorFailure(CoordinatorException e, String op,
                                                    CoordinatorClient client, String memberId,
                                                    String topic, Session session,
                                                    AtomicBoolean fenced) {
        RejectionReason reason = e.reason();
        System.out.println("REJECTED\t" + op + "\t" + reason.name());
        System.out.flush();
        if (reason == RejectionReason.FENCED_INSTANCE) {
            fenced.set(true);
            return true;
        }
        if (reason == RejectionReason.REBALANCE_IN_PROGRESS
                || reason == RejectionReason.STALE_GENERATION
                || reason == RejectionReason.ILLEGAL_GENERATION
                || reason == RejectionReason.UNKNOWN_MEMBER) {
            try {
                rejoin(client, memberId, session);
            } catch (CoordinatorException ce) {
                if (ce.reason() == RejectionReason.FENCED_INSTANCE) {
                    fenced.set(true);
                    System.out.println("REJECTED\tREJOIN\tFENCED_INSTANCE");
                    System.out.flush();
                    return true;
                }
                // Other transient rejoin failures: next loop tick retries.
            } catch (Exception ignored) {
                // transport issue: loop ends via deadline/process tear-down
            }
            return false;
        }
        // NOT_OWNER / OFFSET_REGRESSION: transient for this loop iteration.
        return false;
    }

    private static void rejoin(CoordinatorClient client, String memberId, Session session)
            throws Exception {
        JoinResult joined = client.joinGroup(memberId, session.token);
        JoinResult synced = joined;
        long guard = 0;
        while (synced.status() == JoinStatus.IN_PROGRESS) {
            if (guard++ > 100_000) {
                throw new IllegalStateException("rejoin sync loop did not terminate");
            }
            Thread.sleep(10);
            synced = client.syncGroup(memberId, session.token, joined.generation());
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
