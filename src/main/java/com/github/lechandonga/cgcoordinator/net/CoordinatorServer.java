package com.github.lechandonga.cgcoordinator.net;

import com.github.lechandonga.cgcoordinator.group.GroupCoordinator;
import com.github.lechandonga.cgcoordinator.model.Assignment;
import com.github.lechandonga.cgcoordinator.model.CommitResult;
import com.github.lechandonga.cgcoordinator.model.Snapshot;
import com.github.lechandonga.cgcoordinator.reject.CoordinatorException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Multi-process TCP front end for a single {@link GroupCoordinator}.
 *
 * <p>Each accepted socket is handled by a dedicated thread that processes
 * request lines serially. All requests funnel through the coordinator's
 * single monitor, so concurrency across processes is serialized exactly as
 * within one process. A background reaper periodically expires dead sessions
 * and finalizes join windows, ensuring progress even when no request arrives.
 */
public class CoordinatorServer implements AutoCloseable {

    private final GroupCoordinator coordinator;
    private final int requestedPort;
    private volatile ServerSocket serverSocket;
    private volatile Thread acceptThread;
    private volatile Thread reaperThread;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final long reaperIntervalMillis;

    public CoordinatorServer(GroupCoordinator coordinator, int port) {
        this(coordinator, port, 50L);
    }

    public CoordinatorServer(GroupCoordinator coordinator, int port, long reaperIntervalMillis) {
        this.coordinator = coordinator;
        this.requestedPort = port;
        this.reaperIntervalMillis = reaperIntervalMillis;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            ServerSocket ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress("127.0.0.1", requestedPort));
            this.serverSocket = ss;
        } catch (IOException e) {
            running.set(false);
            throw new IllegalStateException("cannot bind coordinator on port " + requestedPort, e);
        }
        acceptThread = new Thread(this::acceptLoop, "coordinator-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();

        reaperThread = new Thread(this::reaperLoop, "coordinator-reaper");
        reaperThread.setDaemon(true);
        reaperThread.start();
    }

    private void acceptLoop() {
        while (running.get()) {
            final Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                if (running.get()) {
                    // transient; keep serving
                }
                return;
            }
            Thread t = new Thread(() -> handle(socket), "coordinator-client");
            t.setDaemon(true);
            t.start();
        }
    }

    private void reaperLoop() {
        while (running.get()) {
            try {
                Thread.sleep(reaperIntervalMillis);
                long now = System.nanoTime();
                coordinator.expireSessions(now);
                coordinator.tickRebalance(now);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException ignored) {
                // reaper must never die
            }
        }
    }

    private void handle(Socket socket) {
        try (socket;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter out = new BufferedWriter(
                     new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
                String response;
                try {
                    response = dispatch(line);
                } catch (CoordinatorException e) {
                    response = Protocol.err(e.reason().name(), e.getMessage());
                } catch (RuntimeException e) {
                    response = Protocol.err("BAD_REQUEST", e.toString());
                }
                synchronized (out) {
                    out.write(response);
                    out.write('\n');
                    out.flush();
                }
            }
        } catch (IOException ignored) {
            // client closed connection
        }
    }

    String dispatch(String line) {
        String[] f = line.split("\\|", -1);
        if (f.length == 0) {
            return Protocol.err("BAD_REQUEST", "empty request");
        }
        return switch (f[0]) {
            case Protocol.REQ_JOIN -> doJoin(f);
            case Protocol.REQ_HB -> doHeartbeat(f);
            case Protocol.REQ_LEAVE -> doLeave(f);
            case Protocol.REQ_COMMIT -> doCommit(f);
            case Protocol.REQ_FETCH -> doFetch(f);
            case Protocol.REQ_SNAP -> doSnap();
            default -> Protocol.err("BAD_REQUEST", "unknown command " + f[0]);
        };
    }

    private String doJoin(String[] f) {
        if (f.length != 4) {
            return Protocol.err("BAD_REQUEST", "JOIN needs member|incarnation|timeout");
        }
        try {
            long timeout = Long.parseLong(f[3]);
            Assignment a = coordinator.joinGroup(f[1], f[2], timeout);
            String parts = a.partitions().stream().map(String::valueOf)
                    .collect(Collectors.joining(","));
            return Protocol.ok(Long.toString(a.generation()), coordinator.state().name(), parts);
        } catch (CoordinatorException e) {
            return Protocol.err(e.reason().name(), e.getMessage());
        } catch (NumberFormatException e) {
            return Protocol.err("BAD_REQUEST", "timeout not numeric");
        }
    }

    private String doHeartbeat(String[] f) {
        if (f.length != 4) {
            return Protocol.err("BAD_REQUEST", "HB needs member|incarnation|generation");
        }
        try {
            coordinator.heartbeat(f[1], f[2], Long.parseLong(f[3]));
            return Protocol.ok(Long.toString(coordinator.generation()),
                    coordinator.state().name());
        } catch (CoordinatorException e) {
            return Protocol.err(e.reason().name(), e.getMessage());
        } catch (NumberFormatException e) {
            return Protocol.err("BAD_REQUEST", "generation not numeric");
        }
    }

    private String doLeave(String[] f) {
        if (f.length != 4) {
            return Protocol.err("BAD_REQUEST", "LEAVE needs member|incarnation|generation");
        }
        try {
            coordinator.leave(f[1], f[2], Long.parseLong(f[3]));
            return Protocol.ok(Long.toString(coordinator.generation()),
                    coordinator.state().name());
        } catch (CoordinatorException e) {
            return Protocol.err(e.reason().name(), e.getMessage());
        } catch (NumberFormatException e) {
            return Protocol.err("BAD_REQUEST", "generation not numeric");
        }
    }

    private String doCommit(String[] f) {
        if (f.length != 6) {
            return Protocol.err("BAD_REQUEST",
                    "COMMIT needs member|incarnation|generation|partition|offset");
        }
        try {
            long generation = Long.parseLong(f[3]);
            int partition = Integer.parseInt(f[4]);
            long offset = Long.parseLong(f[5]);
            CommitResult r = coordinator.commit(f[1], f[2], generation, partition, offset);
            if (r.accepted()) {
                return Protocol.ok(Long.toString(r.generation()),
                        Integer.toString(r.partition()), Long.toString(r.offset()));
            }
            return Protocol.err(r.rejectReason(), "commit rejected");
        } catch (CoordinatorException e) {
            return Protocol.err(e.reason().name(), e.getMessage());
        } catch (NumberFormatException e) {
            return Protocol.err("BAD_REQUEST", "numeric field invalid");
        }
    }

    private String doFetch(String[] f) {
        if (f.length != 2) {
            return Protocol.err("BAD_REQUEST", "FETCH needs partition");
        }
        try {
            long v = coordinator.fetchOffset(Integer.parseInt(f[1]));
            return Protocol.ok(Long.toString(v));
        } catch (CoordinatorException e) {
            return Protocol.err(e.reason().name(), e.getMessage());
        } catch (NumberFormatException e) {
            return Protocol.err("BAD_REQUEST", "partition not numeric");
        }
    }

    private String doSnap() {
        Snapshot s = coordinator.snapshot();
        String detail = s.assignments().entrySet().stream()
                .map(e -> e.getKey() + ":" + e.getValue().stream()
                        .map(String::valueOf).collect(Collectors.joining(",")))
                .collect(Collectors.joining(";"));
        return Protocol.ok(Long.toString(s.generation()), s.state(), detail);
    }

    public int boundPort() {
        ServerSocket ss = serverSocket;
        return ss == null ? requestedPort : ss.getLocalPort();
    }

    public ServerSocket serverSocket() {
        return serverSocket;
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
            // close best effort
        }
        if (acceptThread != null) {
            acceptThread.interrupt();
        }
        if (reaperThread != null) {
            reaperThread.interrupt();
        }
    }
}
