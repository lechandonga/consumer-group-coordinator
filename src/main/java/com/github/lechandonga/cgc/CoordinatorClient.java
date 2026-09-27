package com.github.lechandonga.cgc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

/**
 * Client side of the {@link CoordinatorServer} protocol: lets a consumer
 * process join, heartbeat, learn its assignment and commit offsets against a
 * coordinator running in another process. Implements {@link Coordinator}, so
 * {@link ConsumerMember} runs unchanged on top of it.
 *
 * <p>Requests are synchronous and serialized; use one client per member.
 */
public class CoordinatorClient implements Coordinator, AutoCloseable {

    private final Socket socket;
    private final BufferedReader in;
    private final PrintWriter out;
    private final CoordinatorConfig config;

    public CoordinatorClient(String host, int port) {
        try {
            this.socket = new Socket(host, port);
            this.in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            this.out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to connect to coordinator at "
                    + host + ":" + port, e);
        }
        String[] cfg = requestOk("CONFIG");
        this.config = new CoordinatorConfig(
                cfg[0], Integer.parseInt(cfg[1]), Long.parseLong(cfg[2]),
                Long.parseLong(cfg[3]), Long.parseLong(cfg[4]),
                Path.of("remote"), RebalanceMode.valueOf(cfg[5]));
    }

    @Override
    public CoordinatorConfig config() {
        return config;
    }

    @Override
    public synchronized HeartbeatResponse joinAndAwait(String memberId)
            throws CoordinatorException {
        return parseAssignment(request("JOINAWAIT " + memberId));
    }

    /** Non-blocking join: may return an in-progress response in BARRIER mode. */
    public synchronized HeartbeatResponse join(String memberId) throws CoordinatorException {
        return parseAssignment(request("JOIN " + memberId));
    }

    @Override
    public synchronized HeartbeatResponse heartbeat(String memberId, long memberEpoch, int generation)
            throws CoordinatorException {
        return parseAssignment(request("HEARTBEAT " + memberId + " " + memberEpoch + " " + generation));
    }

    @Override
    public synchronized void leave(String memberId) throws CoordinatorException {
        request("LEAVE " + memberId);
    }

    @Override
    public synchronized void commit(String memberId, long memberEpoch, int generation,
                                    Partition partition, long offset) throws CoordinatorException {
        request("COMMIT " + memberId + " " + memberEpoch + " " + generation + " "
                + partition.id() + " " + offset);
    }

    @Override
    public synchronized long fetchCommitted(Partition partition) {
        try {
            return Long.parseLong(request("FETCH " + partition.id())[0]);
        } catch (CoordinatorException e) {
            throw new IllegalStateException("FETCH failed unexpectedly", e);
        }
    }

    /** Administrative: current group state line from the server. */
    public synchronized String stateLine() {
        try {
            return String.join(" ", request("STATE"));
        } catch (CoordinatorException e) {
            throw new IllegalStateException("STATE failed unexpectedly", e);
        }
    }

    /** Administrative: ownership-overlap audit result from the server. */
    public synchronized boolean verifyNoOwnershipOverlap() {
        try {
            return Boolean.parseBoolean(request("VERIFY")[0]);
        } catch (CoordinatorException e) {
            throw new IllegalStateException("VERIFY failed unexpectedly", e);
        }
    }

    private String[] request(String line) throws CoordinatorException {
        out.println(line);
        String response;
        try {
            response = in.readLine();
        } catch (IOException e) {
            throw new UncheckedIOException("coordinator connection failed", e);
        }
        if (response == null) {
            throw new UncheckedIOException(
                    new IOException("coordinator closed the connection"));
        }
        String[] parts = response.split(" ", 3);
        if ("ERR".equals(parts[0])) {
            RejectionReason reason;
            try {
                reason = RejectionReason.valueOf(parts[1]);
            } catch (IllegalArgumentException e) {
                reason = RejectionReason.UNKNOWN_MEMBER;
            }
            throw new CoordinatorException(reason, parts.length > 2 ? parts[2] : "");
        }
        // Strip the leading OK token.
        String rest = response.length() <= 3 ? "" : response.substring(3);
        return rest.isEmpty() ? new String[0] : rest.split(" ");
    }

    private String[] requestOk(String line) {
        try {
            return request(line);
        } catch (CoordinatorException e) {
            throw new IllegalStateException("request failed unexpectedly: " + line, e);
        }
    }

    private HeartbeatResponse parseAssignment(String[] tokens) {
        int generation = Integer.parseInt(tokens[0]);
        long epoch = Long.parseLong(tokens[1]);
        boolean inProgress = "1".equals(tokens[2]);
        Set<Partition> assignment = new HashSet<>();
        for (int i = 3; i < tokens.length; i++) {
            assignment.add(new Partition(config.topic(), Integer.parseInt(tokens[i])));
        }
        return new HeartbeatResponse(generation, epoch, assignment, inProgress);
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }
}
