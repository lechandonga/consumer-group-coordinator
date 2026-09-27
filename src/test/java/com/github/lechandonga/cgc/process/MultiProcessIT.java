package com.github.lechandonga.cgc.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end tests in which the coordinator and the consumers are genuinely
 * independent OS processes (separate {@code java} invocations), communicating
 * only over loopback TCP. No external services, credentials or paid resources
 * are involved; everything is local and torn down by the test.
 *
 * <p>Observability comes from the one-event-per-line stdout contract of
 * {@link ConsumerProcessMain} ({@code TOKEN}, {@code ASSIGNED},
 * {@code COMMITTED}, {@code REJECTED ..}, {@code TIMEOUT-CLOSED}, {@code FENCED})
 * and the {@code BOUND <port>} line of {@link CoordinatorServerMain}.
 */
class MultiProcessIT {

    @TempDir
    Path dir;

    private static final String CLASSPATH = System.getProperty("java.class.path");
    private static final String JAVA = Path.of(System.getProperty("java.home"), "bin", "java").toString();

    private static final class Proc implements AutoCloseable {
        final Process process;
        final String name;
        final List<String> lines = new CopyOnWriteArrayList<>();
        final List<Thread> pumps = new ArrayList<>();

        Proc(String name, ProcessBuilder pb) throws IOException {
            this.name = name;
            pb.redirectErrorStream(true);
            this.process = pb.start();
            pumps.add(pump(name + ":out", process.getInputStream(), lines));
        }

        int port() {
            String bound = waitForLinePoll(lines, l -> l.startsWith("BOUND "), 15_000,
                    "server BOUND line");
            return Integer.parseInt(bound.substring("BOUND ".length()).trim());
        }

        String token() {
            String t = waitForLinePoll(lines, l -> l.startsWith("TOKEN\t"), 15_000,
                    name + " TOKEN");
            return t.substring("TOKEN\t".length()).trim();
        }

        @Override
        public void close() {
            process.destroy();
            try {
                if (!process.waitFor(5, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                process.destroyForcibly();
            }
            for (Thread t : pumps) {
                t.interrupt();
            }
        }
    }

    private static Thread pump(String name, InputStream in, List<String> sink) {
        Thread t = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    sink.add(line);
                }
            } catch (IOException ignored) {
                // process tearing down
            }
        }, "pump-" + name);
        t.setDaemon(true);
        t.start();
        return t;
    }

    private interface LinePred {
        boolean test(String line);
    }

    /** Polls a process's event log until a matching line appears or time runs out. */
    private static String waitForLinePoll(List<String> lines, LinePred pred, long timeoutMs,
                                          String what) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Optional<String> hit = lines.stream().filter(pred::test).findFirst();
            if (hit.isPresent()) {
                return hit.get();
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted waiting for " + what);
            }
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    private Proc startServer(int partitions, long heartbeatMs, long sessionMs, long rebalanceMs)
            throws IOException {
        Path store = dir.resolve("offsets-" + System.nanoTime() + ".log");
        ProcessBuilder pb = new ProcessBuilder(JAVA, "-cp", CLASSPATH,
                CoordinatorServerMain.class.getName(),
                "0", String.valueOf(partitions),
                String.valueOf(heartbeatMs), String.valueOf(sessionMs),
                String.valueOf(rebalanceMs), store.toString());
        return new Proc("server", pb);
    }

    private Proc startConsumer(int port, String memberId, String token, String... extra)
            throws IOException {
        List<String> cmd = new ArrayList<>(List.of(JAVA, "-cp", CLASSPATH,
                ConsumerProcessMain.class.getName(),
                "127.0.0.1", String.valueOf(port), memberId, token == null ? "-" : token));
        cmd.addAll(List.of(extra));
        return new Proc("consumer-" + memberId, new ProcessBuilder(cmd));
    }

    private static List<String> assignedPartitions(List<String> lines, String memberId) {
        // Last ASSIGNED line for the member wins.
        String last = null;
        for (String l : lines) {
            if (l.startsWith("ASSIGNED\t" + memberId + "\t")) {
                last = l;
            }
        }
        if (last == null) {
            return List.of();
        }
        String csv = last.substring(("ASSIGNED\t" + memberId + "\t").length());
        if (csv.isBlank()) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (String p : csv.split(",")) {
            out.add(p.trim());
        }
        return out;
    }

    @Test
    void independentProcessesJoinSharePartitionsCommitAndCoverAll() throws Exception {
        try (Proc server = startServer(4, 100, 60_000, 5_000)) {
            int port = server.port();
            try (Proc a = startConsumer(port, "a", null, "--duration-ms", "8000");
                 Proc b = startConsumer(port, "b", null, "--duration-ms", "8000")) {
                a.token();
                b.token();

                // Both eventually settle with non-empty assignments.
                waitForLinePoll(a.lines,
                        l -> assignedPartitions(a.lines, "a").size() == 2,
                        15_000, "a assignment of 2");
                waitForLinePoll(b.lines,
                        l -> assignedPartitions(b.lines, "b").size() == 2,
                        15_000, "b assignment of 2");

                List<String> aa = assignedPartitions(a.lines, "a");
                List<String> bb = assignedPartitions(b.lines, "b");
                // Complete, non-overlapping coverage across processes.
                List<String> union = new ArrayList<>(aa);
                union.addAll(bb);
                assertEquals(4, union.size(), "union covers all partitions");
                assertEquals(4, new java.util.HashSet<>(union).size(),
                        "no partition claimed by two processes");

                // Each committed on its own partitions.
                for (String p : aa) {
                    int id = Integer.parseInt(p.substring(1));
                    String wanted = "COMMITTED\ta\t" + p + "\t";
                    waitForLinePoll(a.lines, l -> l.startsWith(wanted), 15_000,
                            "a commit on " + p);
                    assertTrue(a.lines.stream().anyMatch(l ->
                            l.startsWith("COMMITTED\ta\tp" + id + "\t")));
                }
            }
        }
    }

    @Test
    void deadProcessPartitionsAreTakenOverBySurvivor() throws Exception {
        try (Proc server = startServer(4, 100, 800, 5_000)) {
            int port = server.port();
            Proc a = startConsumer(port, "a", null, "--duration-ms", "20000");
            Proc b = startConsumer(port, "b", null, "--duration-ms", "20000");
            try (a; b) {
                a.token();
                b.token();
                waitForLinePoll(a.lines, l -> assignedPartitions(a.lines, "a").size() == 2,
                        15_000, "a gets 2 partitions");
                waitForLinePoll(b.lines, l -> assignedPartitions(b.lines, "b").size() == 2,
                        15_000, "b gets 2 partitions");

                // b crashes hard (no leave): kill the OS process.
                b.process.destroyForcibly();
                b.process.waitFor(10, TimeUnit.SECONDS);

                // After b's session expires and the generation closes, a takes
                // over every partition and keeps committing on all four.
                waitForLinePoll(a.lines, l -> assignedPartitions(a.lines, "a").size() == 4,
                        20_000, "a takes over all 4 partitions after b crash");
                for (int id = 0; id < 4; id++) {
                    int pid = id;
                    waitForLinePoll(a.lines,
                            l -> l.startsWith("COMMITTED\ta\tp" + pid + "\t"),
                            20_000, "a commits on p" + pid);
                }
            }
        }
    }

    @Test
    void restartedInstanceTakesOverAndOldProcessIsFenced() throws Exception {
        try (Proc server = startServer(2, 100, 60_000, 3_000)) {
            int port = server.port();
            Proc old = startConsumer(port, "a", null, "--duration-ms", "30000",
                    "--poll-ms", "200");
            try (old) {
                String oldToken = old.token();
                waitForLinePoll(old.lines,
                        l -> l.startsWith("COMMITTED\ta\tp0\t"), 15_000, "old instance commits");

                // A brand-new OS process re-joins with the SAME member id but
                // no token: a fresh instance takes over the identity.
                Proc neo = startConsumer(port, "a", null, "--duration-ms", "6000",
                        "--poll-ms", "200");
                try (neo) {
                    String newToken = neo.token();
                    assertNotEquals(oldToken, newToken);
                    waitForLinePoll(neo.lines, l -> l.startsWith("JOINED\ta\t"),
                            15_000, "new instance joins");
                    waitForLinePoll(neo.lines,
                            l -> l.startsWith("COMMITTED\ta\tp0\t"), 15_000,
                            "new instance commits after takeover");

                    // The old process, still alive and heartbeating/committing,
                    // must be explicitly fenced and stop with the FENCED event.
                    String rejection = waitForLinePoll(old.lines,
                            l -> l.contains("FENCED_INSTANCE"), 20_000,
                            "old instance observes FENCED_INSTANCE");
                    assertTrue(rejection.startsWith("REJECTED\t"),
                            "fencing must be an explicit rejection event: " + rejection);
                    waitForLinePoll(old.lines, l -> l.startsWith("FENCED\ta"),
                            20_000, "old process emits FENCED and exits");
                    // Old process terminates with the dedicated fenced exit code.
                    assertTrue(old.process.waitFor(20, TimeUnit.SECONDS));
                    assertEquals(3, old.process.exitValue(),
                            "fenced instance must exit with code 3");
                }
            }
        }
    }

    @Test
    void slowMemberRebalanceClosesAtBoundAndFastMemberIsToldTimeout() throws Exception {
        // Tight bound so the deterministic close is quick to observe.
        try (Proc server = startServer(4, 100, 60_000, 700)) {
            int port = server.port();
            // Fast member joins and works.
            Proc fast = startConsumer(port, "fast", null, "--duration-ms", "25000",
                    "--poll-ms", "200");
            try (fast) {
                fast.token();
                waitForLinePoll(fast.lines, l -> assignedPartitions(fast.lines, "fast").size() == 4,
                        15_000, "fast owns all 4 initially");

                // Slow member performs phase 1 and never syncs: the generation
                // stays open until the bound, then closes deterministically.
                Proc slow = startConsumer(port, "slow", null, "--slow-join",
                        "--duration-ms", "10000");
                try (slow) {
                    slow.token();
                    waitForLinePoll(slow.lines, l -> l.contains("SLOW"), 15_000,
                            "slow member completed phase 1 only");

                    // The fast member sees the change in progress first.
                    waitForLinePoll(fast.lines, l -> l.startsWith("REBALANCING\tfast\t"),
                            15_000, "fast observes REBALANCING");

                    // Bound elapses: the fast member rejoins and is told the
                    // generation completed by timeout; slow (never rejoined) is
                    // evicted; fast ends up owning everything again.
                    waitForLinePoll(fast.lines, l -> l.startsWith("TIMEOUT-CLOSED\t"),
                            15_000, "fast observes TIMEOUT-CLOSED");
                    waitForLinePoll(fast.lines,
                            l -> assignedPartitions(fast.lines, "fast").size() == 4,
                            15_000, "fast owns all 4 after timeout close");
                    // Fast member keeps committing: progress continues.
                    waitForLinePoll(fast.lines,
                            l -> l.startsWith("COMMITTED\tfast\tp3\t"),
                            15_000, "fast commits after timeout close");
                }
            }
        }
    }
}
