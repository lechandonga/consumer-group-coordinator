package com.github.lechandonga.cgcoordinator.cli;

import com.github.lechandonga.cgcoordinator.assign.RangeAssignor;
import com.github.lechandonga.cgcoordinator.audit.OwnershipAuditLog;
import com.github.lechandonga.cgcoordinator.client.ConsumerClient;
import com.github.lechandonga.cgcoordinator.config.CoordinatorConfig;
import com.github.lechandonga.cgcoordinator.group.GroupCoordinator;
import com.github.lechandonga.cgcoordinator.net.CoordinatorServer;
import com.github.lechandonga.cgcoordinator.offset.FileOffsetStore;

import java.util.List;
import java.util.Map;

/**
 * Command-line entry points.
 *
 * <h2>Coordinator process</h2>
 * <pre>
 *   coordinator --port 9000 --partitions 8 \
 *               --session-ms 10000 --heartbeat-ms 3000 --join-ms 2000 \
 *               --checkpoint-dir ./data
 * </pre>
 *
 * <h2>Consumer process</h2>
 * <pre>
 *   consumer --host 127.0.0.1 --port 9000 --member a \
 *            --heartbeat-ms 3000 --messages 100
 * </pre>
 * The consumer joins, consumes its assigned partitions from the last
 * confirmed offset, heartbeats, and commits each processed offset.
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            usage();
            return;
        }
        switch (args[0]) {
            case "coordinator" -> runCoordinator(tail(args));
            case "consumer" -> runConsumer(tail(args));
            default -> usage();
        }
    }

    private static Map<String, String> parse(String[] args) {
        java.util.HashMap<String, String> m = new java.util.HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (args[i].startsWith("--")) {
                m.put(args[i].substring(2), args[i + 1]);
            }
        }
        return m;
    }

    private static String[] tail(String[] args) {
        String[] t = new String[args.length - 1];
        System.arraycopy(args, 1, t, 0, t.length);
        return t;
    }

    private static void runCoordinator(String[] args) {
        Map<String, String> p = parse(args);
        int port = Integer.parseInt(p.getOrDefault("port", "9000"));
        int partitions = Integer.parseInt(p.getOrDefault("partitions", "8"));
        long sessionMs = Long.parseLong(p.getOrDefault("session-ms", "10000"));
        long heartbeatMs = Long.parseLong(p.getOrDefault("heartbeat-ms", "3000"));
        long joinMs = Long.parseLong(p.getOrDefault("join-ms", "2000"));
        String dir = p.getOrDefault("checkpoint-dir", "./data");

        CoordinatorConfig config = new CoordinatorConfig(partitions, sessionMs,
                heartbeatMs, joinMs, dir);
        GroupCoordinator coordinator = new GroupCoordinator(config, new RangeAssignor(),
                new FileOffsetStore(dir, partitions), new OwnershipAuditLog());
        try (CoordinatorServer server = new CoordinatorServer(coordinator, port)) {
            server.start();
            System.out.println("coordinator listening on 127.0.0.1:" + server.boundPort()
                    + " partitions=" + partitions);
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void runConsumer(String[] args) {
        Map<String, String> p = parse(args);
        String host = p.getOrDefault("host", "127.0.0.1");
        int port = Integer.parseInt(p.getOrDefault("port", "9000"));
        String member = p.getOrDefault("member", "consumer-" + ProcessHandle.current().pid());
        long heartbeatMs = Long.parseLong(p.getOrDefault("heartbeat-ms", "3000"));
        long messages = Long.parseLong(p.getOrDefault("messages", "100"));
        long joinMs = Long.parseLong(p.getOrDefault("join-ms", "5000"));

        try (ConsumerClient client = new ConsumerClient(host, port, member)) {
            client.startHeartbeat(heartbeatMs);
            client.join(joinMs);
            // Let the initial membership burst settle into one stable
            // generation before consuming, so we don't process an assignment
            // that is about to be revoked.
            try {
                Thread.sleep(Math.max(joinMs, 1L));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            List<Integer> assigned = client.join(joinMs);
            System.out.println(member + " joined generation " + client.generation()
                    + " partitions=" + assigned);
            long produced = 0;
            // Consume across generations; whenever the heartbeat signals a
            // rebalance, re-join and switch to the new assignment.
            while (produced < messages) {
                for (int p0 : client.partitions()) {
                    long offset = Math.max(0, client.fetchOffset(p0));
                    for (long o = offset; o < offset + messages && produced < messages; o++) {
                        if (client.needsRejoin()) {
                            break;
                        }
                        // "consume": processing is a no-op in this reference impl.
                        var result = client.commit(p0, o + 1);
                        if (!result.accepted()) {
                            // fencing: drop this generation's work and re-join
                            System.err.println(member + " commit rejected: "
                                    + result.rejectReason() + " gen=" + client.generation()
                                    + " partition=" + p0 + " -> re-joining");
                            break;
                        }
                        produced++;
                    }
                    if (client.needsRejoin()) {
                        break;
                    }
                }
                if (client.needsRejoin() || produced < messages) {
                    client.join(joinMs);
                    System.out.println(member + " now generation " + client.generation()
                            + " partitions=" + client.partitions());
                }
            }
            client.leave();
            System.out.println(member + " done");
        }
    }

    private static void usage() {
        System.out.println("""
                Usage:
                  coordinator --port N --partitions N --session-ms N --heartbeat-ms N \
                --join-ms N --checkpoint-dir DIR
                  consumer --host H --port N --member ID --heartbeat-ms N --messages N
                Invalid configuration values are rejected at startup.""");
    }
}
