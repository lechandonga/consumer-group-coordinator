package com.github.lechandonga.cgc.process;

import com.github.lechandonga.cgc.CoordinatorConfig;
import com.github.lechandonga.cgc.server.CoordinatorServer;
import java.nio.file.Path;

/**
 * Entry point: runs the coordinator as an independent process.
 *
 * <pre>
 *   java ... CoordinatorServerMain &lt;port(0=ephemeral)&gt; &lt;partitionCount&gt;
 *        &lt;heartbeatIntervalMs&gt; &lt;sessionTimeoutMs&gt; &lt;rebalanceTimeoutMs&gt;
 *        &lt;offsetStorePath&gt; [topic]
 * </pre>
 *
 * The bound port is printed to stdout as {@code BOUND &lt;port&gt;} (the line is
 * flushed immediately so spawning processes can discover an ephemeral port).
 * The process serves until killed; offsets are persisted to the given path and
 * survive a coordinator restart.
 */
public final class CoordinatorServerMain {

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("usage: CoordinatorServerMain <port> <partitionCount> "
                    + "<heartbeatIntervalMs> <sessionTimeoutMs> <rebalanceTimeoutMs> "
                    + "<offsetStorePath> [topic]");
            System.exit(2);
        }
        int port = Integer.parseInt(args[0]);
        int partitions = Integer.parseInt(args[1]);
        long heartbeat = Long.parseLong(args[2]);
        long session = Long.parseLong(args[3]);
        long rebalance = Long.parseLong(args[4]);
        Path store = Path.of(args[5]);
        String topic = args.length >= 7 ? args[6] : "topic";

        CoordinatorConfig config = new CoordinatorConfig(
                topic, partitions, heartbeat, session, rebalance, store);
        CoordinatorServer server = new CoordinatorServer(config, port);
        int bound = server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
            } catch (RuntimeException ignored) {
                // best effort during shutdown
            }
        }, "coordinator-shutdown"));

        System.out.println("BOUND " + bound);
        System.out.flush();

        // Serve forever: block on an interval that wakes on interrupt.
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private CoordinatorServerMain() {
    }
}
