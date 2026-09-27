package com.github.lechandonga.cgc.client;

import com.github.lechandonga.cgc.CoordinatorException;
import com.github.lechandonga.cgc.HeartbeatResponse;
import com.github.lechandonga.cgc.JoinResult;
import com.github.lechandonga.cgc.JoinStatus;
import com.github.lechandonga.cgc.Partition;
import com.github.lechandonga.cgc.RejectionReason;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Client for {@link com.github.lechandonga.cgc.server.CoordinatorServer}.
 * One instance per consumer process; holds a single persistent TCP connection
 * and serializes request/response pairs over it.
 *
 * <p>The coordinator's topic is fixed in the server configuration; the client
 * needs it only to reconstruct {@link Partition} values from assignment lists.
 */
public final class CoordinatorClient implements Closeable {

    private final String topic;
    private final Socket socket;
    private final BufferedWriter out;
    private final BufferedReader in;

    /** Connects using the default topic name used by the server main. */
    public CoordinatorClient(String host, int port) throws IOException {
        this(host, port, "topic");
    }

    public CoordinatorClient(String host, int port, String topic) throws IOException {
        this.topic = topic;
        this.socket = new Socket(host, port);
        this.socket.setTcpNoDelay(true);
        this.out = new BufferedWriter(
                new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        this.in = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
    }

    /**
     * Join phase 1. A null/blank {@code instanceToken} means "I am a fresh
     * instance of this member id"; the returned token must be presented on
     * every subsequent request of this instance.
     */
    public JoinResult joinGroup(String memberId, String instanceToken)
            throws IOException, CoordinatorException {
        String token = instanceToken == null || instanceToken.isBlank()
                ? "-" : instanceToken;
        return parseJoin(request("JOIN\t" + memberId + "\t" + token));
    }

    /** Phase 2: obtains the assignment once the generation closes; IN_PROGRESS otherwise. */
    public JoinResult syncGroup(String memberId, String instanceToken, int generation)
            throws IOException, CoordinatorException {
        return parseJoin(request("SYNC\t" + memberId + "\t" + instanceToken + "\t" + generation));
    }

    public HeartbeatResponse heartbeat(String memberId, String instanceToken, int generation)
            throws IOException, CoordinatorException {
        String line = request(
                "HEARTBEAT\t" + memberId + "\t" + instanceToken + "\t" + generation);
        String[] f = split(line);
        expect(f, "OK HB", 3);
        int gen = Integer.parseInt(f[1]);
        return new HeartbeatResponse(gen, parsePartitions(f[2]));
    }

    public void commit(String memberId, String instanceToken, int generation,
                       Partition partition, long offset)
            throws IOException, CoordinatorException {
        request("COMMIT\t" + memberId + "\t" + instanceToken + "\t" + generation
                + "\t" + partition.id() + "\t" + offset);
    }

    public void leave(String memberId, String instanceToken) throws IOException {
        // A leave failing at the coordinator level (already unknown/fenced) is
        // fine for shutdown semantics; only transport failures propagate.
        try {
            request("LEAVE\t" + memberId + "\t" + instanceToken);
        } catch (CoordinatorException ignored) {
            // best-effort goodbye
        }
    }

    public long fetchCommitted(Partition partition) throws IOException {
        String line;
        try {
            line = request("FETCH\t" + partition.id());
        } catch (CoordinatorException e) {
            throw new IllegalStateException("FETCH cannot be rejected", e);
        }
        String[] f = split(line);
        expect(f, "OK COMMITTED", 2);
        return Long.parseLong(f[1]);
    }

    public void ping() throws IOException {
        String line;
        try {
            line = request("PING");
        } catch (CoordinatorException e) {
            throw new IllegalStateException("PING cannot be rejected", e);
        }
        if (!"OK PONG".equals(line)) {
            throw new IOException("unexpected ping response: " + line);
        }
    }

    // ------------------------------------------------------------------

    private synchronized String request(String requestLine)
            throws IOException, CoordinatorException {
        if (socket.isClosed()) {
            throw new IOException("connection is closed");
        }
        out.write(requestLine);
        out.newLine();
        out.flush();
        String response = in.readLine();
        if (response == null) {
            throw new IOException("coordinator closed the connection");
        }
        if (!response.isEmpty() && response.charAt(response.length() - 1) == '\r') {
            response = response.substring(0, response.length() - 1);
        }
        if (response.startsWith("ERR\t") || response.startsWith("ERR ")) {
            throw toException(response);
        }
        return response;
    }

    private JoinResult parseJoin(String line) throws IOException {
        String[] f = split(line);
        // Token f0="OK JOIN"; STABLE responses also carry a 6th field (partition
        // id csv); IN_PROGRESS responses leave it empty.
        if (f.length < 5) {
            throw new IOException("bad join response: " + line);
        }
        expectPrefix(f, "OK JOIN");
        JoinStatus status;
        try {
            status = JoinStatus.valueOf(f[1]);
        } catch (IllegalArgumentException e) {
            throw new IOException("bad join status in response: " + line);
        }
        int gen = Integer.parseInt(f[2]);
        String token = f[3];
        boolean byTimeout = "1".equals(f[4]);
        String csv = f.length >= 6 ? f[5] : "";
        Set<Partition> partitions = status == JoinStatus.STABLE ? parsePartitions(csv) : Set.of();
        return new JoinResult(status, gen, token, partitions, byTimeout);
    }

    private Set<Partition> parsePartitions(String csv) {
        Set<Partition> result = new LinkedHashSet<>();
        if (!csv.isEmpty()) {
            for (String id : csv.split(",")) {
                result.add(new Partition(topic, Integer.parseInt(id)));
            }
        }
        return result;
    }

    private static String[] split(String line) {
        return line.split("\t", -1);
    }

    private static void expectPrefix(String[] f, String token) throws IOException {
        if (f.length < 1 || !token.equals(f[0])) {
            throw new IOException("unexpected response: " + String.join("\t", f));
        }
    }

    private static void expect(String[] f, String token, int minFields) throws IOException {
        if (f.length < minFields || !token.equals(f[0])) {
            throw new IOException("unexpected response: " + String.join("\t", f));
        }
    }

    private static CoordinatorException toException(String line) {
        // "ERR" <tab/space> REASON [message...]
        String rest = line.substring(3).stripLeading();
        int sep = rest.indexOf('\t');
        String reasonPart;
        String message;
        if (sep < 0) {
            int sp = rest.indexOf(' ');
            if (sp < 0) {
                reasonPart = rest;
                message = rest;
            } else {
                reasonPart = rest.substring(0, sp);
                message = rest.substring(sp + 1);
            }
        } else {
            reasonPart = rest.substring(0, sep);
            message = rest.substring(sep + 1);
        }
        RejectionReason reason;
        try {
            reason = RejectionReason.valueOf(reasonPart);
        } catch (IllegalArgumentException e) {
            reason = RejectionReason.UNKNOWN_MEMBER;
        }
        return new CoordinatorException(reason, message);
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            socket.close();
        } finally {
            // readers/writers wrap the socket's streams; closing the socket is
            // sufficient and avoids double-close noise.
        }
    }
}
