package com.github.lechandonga.cgc.server;

import com.github.lechandonga.cgc.CoordinatorConfig;
import com.github.lechandonga.cgc.CoordinatorException;
import com.github.lechandonga.cgc.GroupCoordinator;
import com.github.lechandonga.cgc.HeartbeatResponse;
import com.github.lechandonga.cgc.JoinResult;
import com.github.lechandonga.cgc.JoinStatus;
import com.github.lechandonga.cgc.Partition;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * TCP front-end exposing one {@link GroupCoordinator} to independent local
 * processes using a tiny line-based text protocol.
 *
 * <p>Requests (one per line, fields tab-separated; {@code -} denotes blank):
 * <pre>
 *   JOIN     &lt;memberId&gt;\t&lt;token|-&gt;
 *   SYNC     &lt;memberId&gt;\t&lt;token&gt;\t&lt;generation&gt;
 *   HEARTBEAT&lt;memberId&gt;\t&lt;token&gt;\t&lt;generation&gt;
 *   COMMIT   &lt;memberId&gt;\t&lt;token&gt;\t&lt;generation&gt;\t&lt;partitionId&gt;\t&lt;offset&gt;
 *   LEAVE    &lt;memberId&gt;\t&lt;token&gt;
 *   FETCH    &lt;partitionId&gt;
 *   PING
 * </pre>
 *
 * <p>Success responses:
 * <pre>
 *   OK JOIN &lt;status&gt;\t&lt;generation&gt;\t&lt;token&gt;\t&lt;completedByTimeout 0|1&gt;\t&lt;ids csv&gt;
 *   OK HB   &lt;generation&gt;\t&lt;ids csv&gt;
 *   OK COMMITTED &lt;offset&gt;
 *   OK PONG
 * </pre>
 * Failures: {@code ERR <REASON> <message>} — the same machine-distinguishable
 * {@link com.github.lechandonga.cgc.RejectionReason} values as the embedded
 * API; transport problems surface to the client as {@link IOException}.
 *
 * <p>Each connection is handled on its own thread with one request in flight;
 * the coordinator serializes all state changes, so the invariants established
 * in-process (complete/non-overlapping assignment, generation fencing,
 * instance fencing) hold across processes unchanged.
 */
public final class CoordinatorServer implements Closeable {

    private final CoordinatorConfig config;
    private final int requestedPort;
    private final GroupCoordinator coordinator;

    private ServerSocket serverSocket;
    private ExecutorService handlers;
    private volatile Thread acceptThread;
    private volatile boolean started;

    public CoordinatorServer(CoordinatorConfig config, int port) {
        this(config, port, new GroupCoordinator(config));
    }

    /** Test/advanced constructor with a pre-built coordinator (e.g. custom clock). */
    public CoordinatorServer(CoordinatorConfig config, int port, GroupCoordinator coordinator) {
        this.config = config;
        this.requestedPort = port;
        this.coordinator = coordinator;
    }

    /** Binds the listening socket and starts accepting; returns the bound port. */
    public int start() {
        if (started) {
            return serverSocket.getLocalPort();
        }
        try {
            serverSocket = new ServerSocket(requestedPort);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to bind coordinator server on port "
                    + requestedPort, e);
        }
        handlers = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "coordinator-client");
            t.setDaemon(true);
            return t;
        });
        started = true;
        Thread t = new Thread(this::acceptLoop, "coordinator-accept");
        t.setDaemon(true);
        acceptThread = t;
        t.start();
        return serverSocket.getLocalPort();
    }

    public int port() {
        return serverSocket == null ? -1 : serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (started) {
            try {
                Socket socket = serverSocket.accept();
                handlers.submit(() -> handle(socket));
            } catch (SocketException e) {
                if (started) {
                    // Unexpected; stop accepting.
                    started = false;
                }
                return;
            } catch (IOException e) {
                if (started) {
                    started = false;
                }
                return;
            }
        }
    }

    private void handle(Socket socket) {
        try (Socket s = socket;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter out = new BufferedWriter(
                     new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8))) {
            socket.setTcpNoDelay(true);
            String line;
            while ((line = in.readLine()) != null) {
                String response;
                try {
                    response = dispatch(line);
                } catch (CoordinatorException e) {
                    response = "ERR " + e.reason().name() + " " + sanitize(e.getMessage());
                } catch (RuntimeException e) {
                    response = "ERR BAD_REQUEST " + sanitize(String.valueOf(e.getMessage()));
                }
                out.write(response);
                out.newLine();
                out.flush();
            }
        } catch (IOException ignored) {
            // Client went away; nothing to clean up beyond try-with-resources.
        }
    }

    private String dispatch(String line) throws CoordinatorException {
        if (line.isEmpty()) {
            throw new IllegalArgumentException("empty request");
        }
        String[] f = line.split("\t", -1);
        String op = f[0];
        return switch (op) {
            case "PING" -> "OK PONG";
            case "JOIN" -> {
                requireFields(f, 3);
                yield join(f[1], blankToNull(f[2]));
            }
            case "SYNC" -> {
                requireFields(f, 4);
                yield joinResult(f[1], coordinator.syncGroup(f[1], f[2], parseInt(f[3], "generation")));
            }
            case "HEARTBEAT" -> {
                requireFields(f, 4);
                HeartbeatResponse r = coordinator.heartbeat(f[1], f[2], parseInt(f[3], "generation"));
                yield "OK HB\t" + r.generation() + "\t" + encodePartitions(r.assignment());
            }
            case "COMMIT" -> {
                requireFields(f, 6);
                coordinator.commit(f[1], f[2], parseInt(f[3], "generation"),
                        new Partition(config.topic(), parseInt(f[4], "partitionId")),
                        parseLong(f[5], "offset"));
                yield "OK COMMITTED";
            }
            case "LEAVE" -> {
                requireFields(f, 3);
                coordinator.leave(f[1], f[2]);
                yield "OK LEFT";
            }
            case "FETCH" -> {
                requireFields(f, 2);
                long v = coordinator.fetchCommitted(
                        new Partition(config.topic(), parseInt(f[1], "partitionId")));
                yield "OK COMMITTED\t" + v;
            }
            default -> throw new IllegalArgumentException("unknown op: " + op);
        };
    }

    private String join(String memberId, String token) throws CoordinatorException {
        JoinResult r = coordinator.joinGroup(memberId, token);
        return joinResult(memberId, r);
    }

    private String joinResult(String memberId, JoinResult r) {
        Set<Partition> mine = r.status() == JoinStatus.STABLE
                ? r.assignment()
                : coordinator.assignmentOf(memberId); // empty while in progress
        List<Integer> ids = mine.stream().map(Partition::id).sorted().toList();
        return "OK JOIN\t" + r.status().name() + "\t" + r.generation() + "\t"
                + r.instanceToken() + "\t" + (r.completedByTimeout() ? "1" : "0") + "\t"
                + ids.stream().map(String::valueOf).collect(Collectors.joining(","));
    }

    private static void requireFields(String[] f, int n) {
        if (f.length < n) {
            throw new IllegalArgumentException("expected " + n + " fields, got " + f.length);
        }
    }

    private static int parseInt(String s, String what) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad " + what + ": " + s);
        }
    }

    private static long parseLong(String s, String what) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad " + what + ": " + s);
        }
    }

    private static String blankToNull(String s) {
        return (s == null || s.isBlank() || "-".equals(s)) ? null : s;
    }

    /** Messages are folded onto one line so the frame stays a single response. */
    private static String sanitize(String message) {
        if (message == null) {
            return "";
        }
        return message.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
    }

    private static String encodePartitions(Set<Partition> partitions) {
        return partitions.stream().map(Partition::id).sorted()
                .map(String::valueOf).collect(Collectors.joining(","));
    }

    @Override
    public void close() {
        started = false;
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
                // best effort
            }
        }
        if (handlers != null) {
            handlers.shutdownNow();
            try {
                handlers.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        coordinator.close();
        Thread t = acceptThread;
        if (t != null) {
            t.interrupt();
        }
    }
}
