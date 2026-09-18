package com.github.lechandonga.cgcoordinator.client;

import com.github.lechandonga.cgcoordinator.model.CommitResult;
import com.github.lechandonga.cgcoordinator.net.Protocol;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Client used by a local consumer process. It:
 * <ul>
 *   <li>joins the group and tracks the current generation + assignment,</li>
 *   <li>renews the session with a background heartbeat thread,</li>
 *   <li>fences every commit with the current generation and incarnation,</li>
 *   <li>fetches the last confirmed offset for a partition after a restart.</li>
 * </ul>
 *
 * <p>A new incarnation id is generated per process start, so after a crash
 * restart the old zombie incarnation (if its socket thread lingers) is fenced
 * by the coordinator with a distinguishable error.
 */
public class ConsumerClient implements AutoCloseable {

    /** Thrown when a response cannot be parsed; never represents a rejection. */
    public static class ClientIOException extends RuntimeException {
        ClientIOException(String m, Throwable c) {
            super(m, c);
        }
    }

    private final String host;
    private final int port;
    private final String memberId;
    private final String incarnationId = UUID.randomUUID().toString();
    private final ReentrantLock ioLock = new ReentrantLock();

    private Socket socket;
    private BufferedReader in;
    private BufferedWriter out;

    private volatile long generation = -1;
    private volatile List<Integer> partitions = List.of();
    private volatile String state = "EMPTY";
    private volatile Thread heartbeatThread;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ConsumerClient(String serverHost, int serverPort, String memberId) {
        this.host = serverHost;
        this.port = serverPort;
        if (memberId == null || memberId.isBlank()) {
            throw new IllegalArgumentException("memberId must not be blank");
        }
        this.memberId = memberId;
    }

    public String incarnationId() {
        return incarnationId;
    }

    public String memberId() {
        return memberId;
    }

    private void connect() {
        try {
            Socket s = new Socket();
            s.connect(new InetSocketAddress(host, port), 5_000);
            s.setTcpNoDelay(true);
            this.socket = s;
            this.in = new BufferedReader(
                    new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            this.out = new BufferedWriter(
                    new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new ClientIOException("cannot connect to " + host + ":" + port, e);
        }
    }

    private String request(String line) {
        ioLock.lock();
        try {
            if (socket == null || socket.isClosed()) {
                connect();
            }
            out.write(line);
            out.write('\n');
            out.flush();
            String resp = in.readLine();
            if (resp == null) {
                throw new ClientIOException("server closed connection", null);
            }
            return resp;
        } catch (IOException e) {
            // one reconnect retry
            quietClose();
            connect();
            try {
                out.write(line);
                out.write('\n');
                out.flush();
                String resp = in.readLine();
                if (resp == null) {
                    throw new ClientIOException("server closed connection after retry", null);
                }
                return resp;
            } catch (IOException e2) {
                throw new ClientIOException("request failed after retry: " + line, e2);
            }
        } finally {
            ioLock.unlock();
        }
    }

    /** Join and store the assignment; returns the assigned partition list. */
    public List<Integer> join(long timeoutMillis) {
        String resp = request(Protocol.REQ_JOIN + Protocol.DELIM + memberId
                + Protocol.DELIM + incarnationId + Protocol.DELIM + timeoutMillis);
        String[] f = resp.split("\\|", -1);
        if (f[0].equals(Protocol.ERR)) {
            throw new com.github.lechandonga.cgcoordinator.reject.CoordinatorException(
                    parseReason(f), f.length > 2 ? f[2] : "join rejected");
        }
        this.generation = Long.parseLong(f[1]);
        this.state = f[2];
        List<Integer> parts = new ArrayList<>();
        if (f.length > 3 && !f[3].isEmpty()) {
            for (String p : f[3].split(",")) {
                parts.add(Integer.parseInt(p));
            }
        }
        this.partitions = List.copyOf(parts);
        return partitions;
    }

    private static com.github.lechandonga.cgcoordinator.reject.RejectReason parseReason(String[] f) {
        try {
            return com.github.lechandonga.cgcoordinator.reject.RejectReason.valueOf(f[1]);
        } catch (RuntimeException e) {
            return com.github.lechandonga.cgcoordinator.reject.RejectReason.BAD_REQUEST;
        }
    }

    /** Start background session renewal at the given cadence. */
    public void startHeartbeat(long intervalMillis) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        heartbeatThread = new Thread(() -> {
            while (running.get()) {
                try {
                    Thread.sleep(intervalMillis);
                    heartbeatOnce();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (RuntimeException e) {
                    // A REBALANCING reply means we must re-join; surface via state flag.
                    if (e instanceof com.github.lechandonga.cgcoordinator.reject.CoordinatorException ce
                            && (ce.reason()
                                == com.github.lechandonga.cgcoordinator.reject.RejectReason.REBALANCING
                                || ce.reason()
                                == com.github.lechandonga.cgcoordinator.reject.RejectReason.STALE_GENERATION)) {
                        this.state = "REBALANCING";
                    }
                }
            }
        }, "heartbeat-" + memberId);
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    /** One explicit heartbeat; returns the coordinator's current state. */
    public String heartbeatOnce() {
        String resp = request(Protocol.REQ_HB + Protocol.DELIM + memberId
                + Protocol.DELIM + incarnationId + Protocol.DELIM + generation);
        String[] f = resp.split("\\|", -1);
        if (f[0].equals(Protocol.ERR)) {
            throw new com.github.lechandonga.cgcoordinator.reject.CoordinatorException(
                    parseReason(f), f.length > 2 ? f[2] : "heartbeat rejected");
        }
        this.state = f[2];
        return this.state;
    }

    public boolean needsRejoin() {
        return "REBALANCING".equals(state);
    }

    /** Commit fenced by current generation/incarnation; never throws for rejection. */
    public CommitResult commit(int partition, long offset) {
        String resp = request(Protocol.REQ_COMMIT + Protocol.DELIM + memberId
                + Protocol.DELIM + incarnationId + Protocol.DELIM + generation
                + Protocol.DELIM + partition + Protocol.DELIM + offset);
        String[] f = resp.split("\\|", -1);
        if (f[0].equals(Protocol.ERR)) {
            return CommitResult.rejected(memberId, partition, offset, generation, f[1]);
        }
        long gen = Long.parseLong(f[1]);
        int p = Integer.parseInt(f[2]);
        long o = Long.parseLong(f[3]);
        return CommitResult.accepted(memberId, p, o, gen);
    }

    public long fetchOffset(int partition) {
        String resp = request(Protocol.REQ_FETCH + Protocol.DELIM + partition);
        String[] f = resp.split("\\|", -1);
        if (f[0].equals(Protocol.ERR)) {
            throw new com.github.lechandonga.cgcoordinator.reject.CoordinatorException(
                    parseReason(f), f.length > 2 ? f[2] : "fetch rejected");
        }
        return Long.parseLong(f[1]);
    }

    public void leave() {
        if (generation < 0) {
            return;
        }
        try {
            request(Protocol.REQ_LEAVE + Protocol.DELIM + memberId
                    + Protocol.DELIM + incarnationId + Protocol.DELIM + generation);
        } finally {
            generation = -1;
            partitions = List.of();
        }
    }

    public long generation() {
        return generation;
    }

    public List<Integer> partitions() {
        return partitions;
    }

    public String state() {
        return state;
    }

    private void quietClose() {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
            // best effort
        }
    }

    @Override
    public void close() {
        running.set(false);
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
        }
        quietClose();
    }
}
