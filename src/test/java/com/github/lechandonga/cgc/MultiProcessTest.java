package com.github.lechandonga.cgc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end multi-process tests: a coordinator process and consumer
 * processes as separate JVMs on loopback. No external services involved.
 */
class MultiProcessTest {

    private static final long AWAIT_MS = 30_000;

    @TempDir
    Path dir;

    /** A spawned child JVM with its stdout drained into a line queue. */
    private static final class TestProcess implements AutoCloseable {
        private final Process process;
        private final BlockingQueue<String> lines = new LinkedBlockingQueue<>();
        private final List<String> allLines = new ArrayList<>();

        TestProcess(String mainClass, String... args) throws Exception {
            String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            List<String> command = new ArrayList<>(List.of(
                    java, "-cp", System.getProperty("java.class.path"), mainClass));
            command.addAll(List.of(args));
            process = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    process.getInputStream(), StandardCharsets.UTF_8));
            Thread drainer = new Thread(() -> {
                try {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        synchronized (allLines) {
                            allLines.add(line);
                        }
                        lines.offer(line);
                    }
                } catch (Exception ignored) {
                }
            });
            drainer.setDaemon(true);
            drainer.start();
        }

        /** Waits for a stdout line starting with the given prefix. */
        String awaitLine(String prefix) throws InterruptedException {
            long deadline = System.currentTimeMillis() + AWAIT_MS;
            List<String> skipped = new ArrayList<>();
            try {
                while (true) {
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0) {
                        throw new AssertionError("no line starting with '" + prefix
                                + "' within " + AWAIT_MS + "ms; saw: " + snapshot());
                    }
                    String line = lines.poll(remaining, TimeUnit.MILLISECONDS);
                    if (line == null) {
                        continue;
                    }
                    if (line.startsWith(prefix)) {
                        return line;
                    }
                    skipped.add(line);
                }
            } finally {
                lines.addAll(skipped); // keep non-matching lines for later awaits
            }
        }

        /** Latest line with the given prefix seen so far, or null. */
        String latest(String prefix) {
            synchronized (allLines) {
                for (int i = allLines.size() - 1; i >= 0; i--) {
                    if (allLines.get(i).startsWith(prefix)) {
                        return allLines.get(i);
                    }
                }
            }
            return null;
        }

        List<String> snapshot() {
            synchronized (allLines) {
                return List.copyOf(allLines);
            }
        }

        boolean exited() {
            return !process.isAlive();
        }

        void killForcibly() {
            process.destroyForcibly();
        }

        @Override
        public void close() {
            process.destroyForcibly();
        }
    }

    private TestProcess startServer(Path store, int partitions) throws Exception {
        TestProcess server = new TestProcess(CoordinatorServerMain.class.getName(),
                "--port", "0",
                "--partitions", String.valueOf(partitions),
                "--store", store.toString(),
                "--heartbeat-ms", "100",
                "--session-ms", "1000",
                "--rebalance-ms", "2000",
                "--mode", "BARRIER");
        return server;
    }

    private static Map<Integer, String> parsePartsPerMember(List<TestProcess> members) {
        Map<Integer, String> owner = new HashMap<>();
        for (TestProcess member : members) {
            String assigned = member.latest("ASSIGNED");
            if (assigned == null) {
                continue;
            }
            String id = assigned.split(" ")[1].substring(3);
            String parts = assigned.substring(assigned.indexOf('[') + 1, assigned.indexOf(']'));
            if (parts.isEmpty()) {
                continue;
            }
            for (String p : parts.split(",")) {
                owner.put(Integer.parseInt(p), id);
            }
        }
        return owner;
    }

    private static Map<Integer, Long> parsePositions(String positionsLine) {
        Map<Integer, Long> positions = new HashMap<>();
        String payload = positionsLine.substring(positionsLine.indexOf(' ') + 1);
        payload = payload.substring(payload.indexOf(' ') + 1);
        if (payload.isBlank()) {
            return positions;
        }
        for (String kv : payload.split(",")) {
            String[] parts = kv.split("=");
            positions.put(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
        }
        return positions;
    }

    @Test
    void multipleProcessesFormOneGroup() throws Exception {
        try (TestProcess server = startServer(dir.resolve("offsets.log"), 4)) {
            String listening = server.awaitLine("LISTENING");
            int port = Integer.parseInt(listening.split(" ")[1]);

            TestProcess m1 = new TestProcess(ConsumerMemberMain.class.getName(),
                    "--host", "127.0.0.1", "--port", String.valueOf(port), "--id", "m1");
            TestProcess m2 = new TestProcess(ConsumerMemberMain.class.getName(),
                    "--host", "127.0.0.1", "--port", String.valueOf(port), "--id", "m2");
            try {
                m1.awaitLine("ASSIGNED");
                m2.awaitLine("ASSIGNED");

                // Eventually: complete, disjoint coverage across the two processes.
                awaitCondition(() -> {
                    Map<Integer, String> owners = parsePartsPerMember(List.of(m1, m2));
                    return owners.size() == 4
                            && owners.values().stream().distinct().count() == 2;
                }, "complete disjoint coverage across processes");

                // Both processes make progress on their own partitions.
                awaitCondition(() -> {
                    Set<Integer> progressed = new HashSet<>();
                    for (TestProcess m : List.of(m1, m2)) {
                        String line = m.latest("POSITIONS");
                        if (line != null) {
                            parsePositions(line).forEach((p, o) -> {
                                if (o >= 1) {
                                    progressed.add(p);
                                }
                            });
                        }
                    }
                    return progressed.size() == 4;
                }, "all partitions progressed by their owning process");

                // Server-side audit: no partition ever had two live owners.
                try (CoordinatorClient admin = new CoordinatorClient("127.0.0.1", port)) {
                    assertTrue(admin.verifyNoOwnershipOverlap());
                    assertEquals("STABLE", admin.stateLine().split(" ")[0]);
                }
            } finally {
                m1.close();
                m2.close();
            }
        }
    }

    @Test
    void crashedProcessPartitionsAreTakenOver() throws Exception {
        try (TestProcess server = startServer(dir.resolve("offsets-crash.log"), 4)) {
            int port = Integer.parseInt(server.awaitLine("LISTENING").split(" ")[1]);

            TestProcess m1 = new TestProcess(ConsumerMemberMain.class.getName(),
                    "--host", "127.0.0.1", "--port", String.valueOf(port), "--id", "m1");
            TestProcess m2 = new TestProcess(ConsumerMemberMain.class.getName(),
                    "--host", "127.0.0.1", "--port", String.valueOf(port), "--id", "m2");
            try {
                m1.awaitLine("ASSIGNED");
                m2.awaitLine("ASSIGNED");
                awaitCondition(() -> parsePartsPerMember(List.of(m1, m2)).size() == 4,
                        "initial coverage");

                // m1 crashes (SIGKILL: no leave, no goodbye).
                m1.killForcibly();

                // m2 takes over all four partitions and keeps progressing them.
                awaitCondition(() -> {
                    String assigned = m2.latest("ASSIGNED");
                    if (assigned == null) {
                        return false;
                    }
                    String parts = assigned.substring(assigned.indexOf('[') + 1,
                            assigned.indexOf(']'));
                    return parts.split(",").length == 4;
                }, "surviving process takes over all partitions");

                awaitCondition(() -> {
                    String line = m2.latest("POSITIONS");
                    return line != null && parsePositions(line).size() == 4
                            && parsePositions(line).values().stream().allMatch(o -> o >= 1);
                }, "surviving process progresses the crashed member's partitions");

                try (CoordinatorClient admin = new CoordinatorClient("127.0.0.1", port)) {
                    assertTrue(admin.verifyNoOwnershipOverlap());
                    assertTrue(admin.stateLine().contains("m2"));
                }
            } finally {
                m1.close();
                m2.close();
            }
        }
    }

    @Test
    void replacedInstanceIsFencedAcrossProcesses() throws Exception {
        try (TestProcess server = startServer(dir.resolve("offsets-zombie.log"), 2)) {
            int port = Integer.parseInt(server.awaitLine("LISTENING").split(" ")[1]);

            TestProcess oldInstance = new TestProcess(ConsumerMemberMain.class.getName(),
                    "--host", "127.0.0.1", "--port", String.valueOf(port), "--id", "z");
            try {
                oldInstance.awaitLine("ASSIGNED");
                awaitCondition(() -> oldInstance.latest("POSITIONS") != null,
                        "old instance progresses before replacement");

                // A new instance of the same member id starts elsewhere.
                TestProcess newInstance = new TestProcess(ConsumerMemberMain.class.getName(),
                        "--host", "127.0.0.1", "--port", String.valueOf(port), "--id", "z");
                try {
                    newInstance.awaitLine("ASSIGNED");

                    // The old instance is observably fenced and stops.
                    oldInstance.awaitLine("FENCED");
                    awaitCondition(oldInstance::exited, "fenced process exits");

                    // The new instance cleanly takes over and progresses.
                    awaitCondition(() -> {
                        String line = newInstance.latest("POSITIONS");
                        return line != null && parsePositions(line).size() == 2
                                && parsePositions(line).values().stream().allMatch(o -> o >= 2);
                    }, "new instance takes over and progresses");

                    try (CoordinatorClient admin = new CoordinatorClient("127.0.0.1", port)) {
                        assertTrue(admin.verifyNoOwnershipOverlap());
                        // Committed offsets only moved forward.
                        for (int p = 0; p < 2; p++) {
                            assertTrue(admin.fetchCommitted(new Partition("topic", p)) >= 1);
                        }
                    }
                } finally {
                    newInstance.close();
                }
            } finally {
                oldInstance.close();
            }
        }
    }

    private static void awaitCondition(Check condition, String description)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_MS;
        while (!condition.check()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("condition not met within " + AWAIT_MS + "ms: "
                        + description);
            }
            Thread.sleep(50);
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean check();
    }
}
