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
class MultiProcessTest {

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
            // Tests need deterministic teardown, not graceful shutdown: kill the
            // OS process hard and wait for it so no child lingers between tests.
            process.destroyForcibly();
            try {
                process.waitFor(5, TimeUnit.SECONDS);
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

    private static List<String> assignedPartitions(List<String> lines, String memberId) {        // Last ASSIGNED line for the member wins.
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

    /** Generation carried by the member's most recent accepted commit, or null. */
    private static Integer latestCommitGeneration(List<String> lines, String memberId) {
        Integer gen = null;
        for (String l : lines) {
            String prefix = "COMMITTED\t" + memberId + "\t";
            if (l.startsWith(prefix) && l.contains("\tgen")) {
                String tail = l.substring(l.lastIndexOf("gen") + 3);
                try {
                    gen = Integer.parseInt(tail.trim());
                } catch (NumberFormatException ignored) {
                    // keep previous
                }
            }
        }
        return gen;
    }

    @Test
    void independentProcessesJoinSharePartitionsCommitAndCoverAll() throws Exception {
        try (Proc server = startServer(4, 100, 60_000, 5_000)) {
            int port = server.port();
            // Bring the first consumer up and wait until it stably owns the
            // whole group, then add the second process. This mirrors a real
            // rollout and avoids two phase-1s racing the bootstrap generation.
            Proc a = startConsumer(port, "a", null, "--duration-ms", "30000");
            try (a) {
                waitForLinePoll(a.lines,
                        l -> assignedPartitions(a.lines, "a").size() == 4
                                && latestCommitGeneration(a.lines, "a") != null,
                        15_000, "a owns all 4 before b joins");
                try (Proc b = startConsumer(port, "b", null, "--duration-ms", "30000")) {
                    b.token();

                    // Wait until BOTH processes hold complementary partitions
                    // and concurrently commit at the same generation.
                    long deadline = System.currentTimeMillis() + 25_000;
                    boolean settled = false;
                    while (System.currentTimeMillis() < deadline) {
                        List<String> aa = assignedPartitions(a.lines, "a");
                        List<String> bb = assignedPartitions(b.lines, "b");
                        java.util.Set<String> union = new java.util.HashSet<>(aa);
                        union.addAll(bb);
                        Integer ga = latestCommitGeneration(a.lines, "a");
                        Integer gb = latestCommitGeneration(b.lines, "b");
                        if (aa.size() == 2 && bb.size() == 2 && union.size() == 4
                                && ga != null && ga.equals(gb)) {
                            settled = true;
                            break;
                        }
                        Thread.sleep(50);
                    }
                    assertTrue(settled, "both processes never held a common complementary "
                            + "assignment; a=" + assignedPartitions(a.lines, "a")
                            + " b=" + assignedPartitions(b.lines, "b"));

                    List<String> aa = assignedPartitions(a.lines, "a");
                    List<String> bb = assignedPartitions(b.lines, "b");
                    java.util.Set<String> union = new java.util.HashSet<>(aa);
                    union.addAll(bb);
                    assertEquals(4, union.size(), "union covers all partitions");
                    assertEquals(aa.size() + bb.size(), union.size(),
                            "no partition claimed by two processes");

                    for (String p : aa) {
                        waitForLinePoll(a.lines,
                                l -> l.startsWith("COMMITTED\ta\t" + p + "\t"),
                                10_000, "a commit on " + p);
                    }
                    for (String p : bb) {
                        waitForLinePoll(b.lines,
                                l -> l.startsWith("COMMITTED\tb\t" + p + "\t"),
                                10_000, "b commit on " + p);
                    }
                }
            }
        }
    }

    @Test
    void deadProcessPartitionsAreTakenOverBySurvivor() throws Exception {
        try (Proc server = startServer(4, 100, 800, 5_000)) {
            int port = server.port();
            // a comes up first and owns everything.
            Proc a = startConsumer(port, "a", null, "--duration-ms", "30000");
            try (a) {
                waitForLinePoll(a.lines,
                        l -> assignedPartitions(a.lines, "a").size() == 4
                                && latestCommitGeneration(a.lines, "a") != null,
                        15_000, "a owns all 4 before b joins");

                Proc b = startConsumer(port, "b", null, "--duration-ms", "30000");
                try (b) {
                    b.token();
                    // Reach a stable two-way split while both are alive.
                    long settleDeadline = System.currentTimeMillis() + 20_000;
                    boolean settled = false;
                    while (System.currentTimeMillis() < settleDeadline) {
                        List<String> aa = assignedPartitions(a.lines, "a");
                        List<String> bb = assignedPartitions(b.lines, "b");
                        Integer ga = latestCommitGeneration(a.lines, "a");
                        Integer gb = latestCommitGeneration(b.lines, "b");
                        if (aa.size() == 2 && bb.size() == 2 && ga != null && ga.equals(gb)) {
                            settled = true;
                            break;
                        }
                        Thread.sleep(50);
                    }
                    assertTrue(settled, "two-way split never stabilised before crash");

                    // b crashes hard (no leave): kill the OS process.
                    b.process.destroyForcibly();
                    b.process.waitFor(10, TimeUnit.SECONDS);

                    // After b's session expires and the generation closes, a
                    // takes over every partition and keeps committing on all 4.
                    waitForLinePoll(a.lines,
                            l -> assignedPartitions(a.lines, "a").size() == 4,
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
            // Fast member joins and works. In observe mode it keeps heartbeating
            // through any rebalance but never issues phase 1 for it, so a
            // straggler-driven window can only close at the time bound.
            Proc fast = startConsumer(port, "fast", null, "--duration-ms", "25000",
                    "--poll-ms", "200", "--observe-rebalance");
            try (fast) {
                fast.token();
                waitForLinePoll(fast.lines, l -> assignedPartitions(fast.lines, "fast").size() == 4,
                        15_000, "fast owns all 4 initially");

                // Slow member performs phase 1, never finishes phase 2 and then
                // goes silent (no heartbeats): the window can only close at the
                // configured bound, never early.
                Proc slow = startConsumer(port, "slow", null, "--slow-join",
                        "--duration-ms", "10000");
                try (slow) {
                    slow.token();
                    waitForLinePoll(slow.lines, l -> l.contains("SLOW"), 15_000,
                            "slow member completed phase 1 only");

                    // The fast member observes an in-progress change first. This
                    // is distinct from a failure: it keeps working/heartbeating.
                    waitForLinePoll(fast.lines, l -> l.startsWith("REBALANCING\tfast\t"),
                            15_000, "fast observes REBALANCING");

                    // The window must not close before the bound (slow never
                    // finishes phase 2). Then the bound elapses: the fast member
                    // is told the generation completed by timeout and, because it
                    // kept heartbeating, it survives and owns every partition
                    // again; the silent slow member is evicted.
                    waitForLinePoll(fast.lines, l -> l.contains("TIMEOUT-CLOSED"),
                            15_000, "fast observes TIMEOUT-CLOSED");
                    waitForLinePoll(fast.lines,
                            l -> assignedPartitions(fast.lines, "fast").size() == 4,
                            15_000, "fast owns all 4 after timeout close");
                    // Fast member keeps committing after the deterministic close.
                    waitForLinePoll(fast.lines,
                            l -> l.startsWith("COMMITTED\tfast\tp3\t"),
                            15_000, "fast commits after timeout close");
                }
            }
        }
    }

    private static List<String> combinedLines(Proc... procs) {
        List<String> all = new ArrayList<>();
        for (Proc p : procs) {
            all.addAll(p.lines);
        }
        return all;
    }

    /** Like {@link #waitForLinePoll} but re-scans several live process logs. */
    private static String waitForLineAny(long timeoutMs, String what, LinePred pred,
                                         Proc... procs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (Proc p : procs) {
                Optional<String> hit = p.lines.stream().filter(pred::test).findFirst();
                if (hit.isPresent()) {
                    return hit.get();
                }
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

    @Test
    void simultaneousStartConsumersConvergeWithoutDying() throws Exception {
        // Three consumer processes start at (almost) the same time: their
        // phase-1 joins and phase-2 confirms interleave and supersede each
        // other's windows. No process may die because of this; all must end
        // up with a complete, non-overlapping assignment and keep committing.
        try (Proc server = startServer(6, 100, 60_000, 2_000)) {
            int port = server.port();
            Proc a = startConsumer(port, "a", null, "--duration-ms", "30000");
            Proc b = startConsumer(port, "b", null, "--duration-ms", "30000");
            Proc c = startConsumer(port, "c", null, "--duration-ms", "30000");
            try (a; b; c) {
                // The interleaving must be observable as in-progress
                // (REBALANCING), which is distinct from a final failure.
                waitForLineAny(20_000, "some member observes the in-progress rebalance",
                        l -> l.startsWith("REBALANCING\t"), a, b, c);

                // Converge: 2 partitions each, union covers all 6, and every
                // process commits at a common generation.
                long deadline = System.currentTimeMillis() + 30_000;
                boolean settled = false;
                while (System.currentTimeMillis() < deadline) {
                    List<String> pa = assignedPartitions(a.lines, "a");
                    List<String> pb = assignedPartitions(b.lines, "b");
                    List<String> pc = assignedPartitions(c.lines, "c");
                    java.util.Set<String> union = new java.util.HashSet<>(pa);
                    union.addAll(pb);
                    union.addAll(pc);
                    Integer ga = latestCommitGeneration(a.lines, "a");
                    Integer gb = latestCommitGeneration(b.lines, "b");
                    Integer gc = latestCommitGeneration(c.lines, "c");
                    if (pa.size() == 2 && pb.size() == 2 && pc.size() == 2
                            && union.size() == 6
                            && ga != null && ga.equals(gb) && gb.equals(gc)) {
                        settled = true;
                        break;
                    }
                    Thread.sleep(50);
                }
                assertTrue(settled, "simultaneous starters never converged: a="
                        + assignedPartitions(a.lines, "a") + " b="
                        + assignedPartitions(b.lines, "b") + " c="
                        + assignedPartitions(c.lines, "c"));

                // Nobody died because of the interleaving, and nobody logged
                // an uncaught exception.
                assertTrue(a.process.isAlive(), "consumer a exited unexpectedly");
                assertTrue(b.process.isAlive(), "consumer b exited unexpectedly");
                assertTrue(c.process.isAlive(), "consumer c exited unexpectedly");
                for (String line : combinedLines(a, b, c)) {
                    assertTrue(!line.contains("Exception") && !line.contains("Error"),
                            "unexpected crash output: " + line);
                }

                // Complete and disjoint right now.
                List<String> pa = assignedPartitions(a.lines, "a");
                List<String> pb = assignedPartitions(b.lines, "b");
                List<String> pc = assignedPartitions(c.lines, "c");
                java.util.Set<String> union = new java.util.HashSet<>(pa);
                union.addAll(pb);
                union.addAll(pc);
                assertEquals(6, union.size(), "union covers all partitions");
                assertEquals(pa.size() + pb.size() + pc.size(), union.size(),
                        "no partition claimed by two processes");
            }
        }
    }

    @Test
    void restartInterleavedWithAnotherMembersChangeConverges() throws Exception {
        // A restart (same member id, fresh instance) lands at the same time as
        // an unrelated new member joins: the two membership changes interleave.
        // The old instance must be permanently fenced (and exit), the new
        // instance and the newcomer must converge to a complete, disjoint
        // assignment and keep committing; nobody crashes.
        try (Proc server = startServer(4, 100, 60_000, 2_000)) {
            int port = server.port();
            Proc oldA = startConsumer(port, "a", null, "--duration-ms", "30000",
                    "--poll-ms", "100");
            try (oldA) {
                String oldToken = oldA.token();
                waitForLinePoll(oldA.lines,
                        l -> assignedPartitions(oldA.lines, "a").size() == 4
                                && latestCommitGeneration(oldA.lines, "a") != null,
                        15_000, "a owns all 4 before the interleaved change");

                // Fire both changes at (almost) the same time: a new instance
                // of a, and a brand-new member b.
                Proc newA = startConsumer(port, "a", null, "--duration-ms", "30000");
                Proc b = startConsumer(port, "b", null, "--duration-ms", "30000");
                try (newA; b) {
                    String newToken = newA.token();
                    assertNotEquals(oldToken, newToken);

                    // The superseded old instance is explicitly fenced and
                    // stops with the dedicated exit code; it never sneaks back.
                    waitForLinePoll(oldA.lines, l -> l.contains("FENCED_INSTANCE"),
                            20_000, "old instance observes FENCED_INSTANCE");
                    waitForLinePoll(oldA.lines, l -> l.startsWith("FENCED\ta"),
                            20_000, "old instance emits FENCED");
                    assertTrue(oldA.process.waitFor(20, TimeUnit.SECONDS));
                    assertEquals(3, oldA.process.exitValue(),
                            "fenced old instance must exit with code 3");

                    // The survivors converge: 2 partitions each, union covers
                    // all 4, both commit at a common generation.
                    long deadline = System.currentTimeMillis() + 30_000;
                    boolean settled = false;
                    while (System.currentTimeMillis() < deadline) {
                        List<String> pa = assignedPartitions(newA.lines, "a");
                        List<String> pb = assignedPartitions(b.lines, "b");
                        java.util.Set<String> union = new java.util.HashSet<>(pa);
                        union.addAll(pb);
                        Integer ga = latestCommitGeneration(newA.lines, "a");
                        Integer gb = latestCommitGeneration(b.lines, "b");
                        if (pa.size() == 2 && pb.size() == 2 && union.size() == 4
                                && ga != null && ga.equals(gb)) {
                            settled = true;
                            break;
                        }
                        Thread.sleep(50);
                    }
                    assertTrue(settled, "new a and b never converged: a="
                            + assignedPartitions(newA.lines, "a") + " b="
                            + assignedPartitions(b.lines, "b"));
                    assertTrue(newA.process.isAlive(), "new a exited unexpectedly");
                    assertTrue(b.process.isAlive(), "b exited unexpectedly");
                    for (String line : combinedLines(newA, b)) {
                        assertTrue(!line.contains("Exception") && !line.contains("Error"),
                                "unexpected crash output: " + line);
                    }
                }
            }
        }
    }
}
