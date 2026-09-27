package com.github.lechandonga.cgc;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Standalone coordinator process. Usage:
 * <pre>
 *   java ... CoordinatorServerMain --port 0 --partitions 4 --store /tmp/offsets.log \
 *        [--topic topic] [--heartbeat-ms 100] [--session-ms 1000] \
 *        [--rebalance-ms 2000] [--mode IMMEDIATE|BARRIER]
 * </pre>
 * Prints {@code LISTENING <port>} once ready, then serves until killed.
 */
public final class CoordinatorServerMain {

    private CoordinatorServerMain() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parse(args);
        int port = Integer.parseInt(opts.getOrDefault("port", "0"));
        CoordinatorConfig config = new CoordinatorConfig(
                opts.getOrDefault("topic", "topic"),
                Integer.parseInt(opts.getOrDefault("partitions", "4")),
                Long.parseLong(opts.getOrDefault("heartbeat-ms", "100")),
                Long.parseLong(opts.getOrDefault("session-ms", "1000")),
                Long.parseLong(opts.getOrDefault("rebalance-ms", "2000")),
                Path.of(opts.getOrDefault("store", "offsets.log")),
                RebalanceMode.valueOf(opts.getOrDefault("mode", "BARRIER")));
        GroupCoordinator coordinator = new GroupCoordinator(config);
        CoordinatorServer server = new CoordinatorServer(coordinator, port);
        System.out.println("LISTENING " + server.port());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            coordinator.close();
        }));
        new CountDownLatch(1).await();
    }

    static Map<String, String> parse(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (args[i].startsWith("--")) {
                opts.put(args[i].substring(2), args[i + 1]);
            }
        }
        return opts;
    }
}
