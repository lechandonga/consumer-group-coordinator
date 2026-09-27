package com.github.lechandonga.cgc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Exposes a {@link GroupCoordinator} to remote consumer processes over TCP.
 *
 * <p>Line-based text protocol (UTF-8), one request per line, one response per
 * line. Requests:
 * <pre>
 *   CONFIG                                  -&gt; OK topic partitions hbMs sessionMs rebalanceMs mode
 *   JOIN memberId                           -&gt; OK gen epoch inProgress(0|1) [partitionId...]
 *   JOINAWAIT memberId                      -&gt; same, but settles any open barrier first
 *   HEARTBEAT memberId epoch gen            -&gt; OK gen epoch inProgress(0|1) [partitionId...]
 *   LEAVE memberId                          -&gt; OK
 *   COMMIT memberId epoch gen partition off -&gt; OK
 *   FETCH partitionId                       -&gt; OK offset
 *   STATE                                   -&gt; OK state gen member,member p=owner,...
 *   VERIFY                                  -&gt; OK true|false  (ownership-overlap audit)
 * </pre>
 * Rejections are returned as {@code ERR REASON message} with the
 * {@link RejectionReason} name, so remote callers observe the same
 * machine-distinguishable outcomes as embedded callers.
 */
public class CoordinatorServer implements AutoCloseable {

    private final GroupCoordinator coordinator;
    private final ServerSocket serverSocket;
    private final Thread acceptThread;
    private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
    private volatile boolean running = true;

    public CoordinatorServer(GroupCoordinator coordinator, int port) {
        this.coordinator = coordinator;
        try {
            this.serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to bind coordinator server", e);
        }
        this.acceptThread = new Thread(this::acceptLoop, "coordinator-server-accept");
        this.acceptThread.setDaemon(true);
        this.acceptThread.start();
    }

    public int port() {
        return serverSocket.getLocalPort();
    }

    public GroupCoordinator coordinator() {
        return coordinator;
    }

    private void acceptLoop() {
        while (running) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                return; // closed
            }
            connections.add(socket);
            Thread t = new Thread(() -> serve(socket), "coordinator-server-client");
            t.setDaemon(true);
            t.start();
        }
    }

    private void serve(Socket socket) {
        try (socket;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8)) {
            String line;
            while ((line = in.readLine()) != null) {
                out.println(handle(line.trim()));
            }
        } catch (IOException ignored) {
            // Client went away.
        } finally {
            connections.remove(socket);
        }
    }

    /** Visible for testing. */
    String handle(String line) {
        if (line.isEmpty()) {
            return "ERR UNKNOWN_MEMBER empty request";
        }
        String[] parts = line.split(" ");
        try {
            return switch (parts[0]) {
                case "CONFIG" -> handleConfig();
                case "JOIN" -> response(coordinator.join(parts[1]));
                case "JOINAWAIT" -> response(joinAwait(parts[1]));
                case "HEARTBEAT" -> response(coordinator.heartbeat(
                        parts[1], Long.parseLong(parts[2]), Integer.parseInt(parts[3])));
                case "LEAVE" -> {
                    coordinator.leave(parts[1]);
                    yield "OK";
                }
                case "COMMIT" -> {
                    coordinator.commit(parts[1], Long.parseLong(parts[2]), Integer.parseInt(parts[3]),
                            new Partition(coordinator.config().topic(), Integer.parseInt(parts[4])),
                            Long.parseLong(parts[5]));
                    yield "OK";
                }
                case "FETCH" -> "OK " + coordinator.fetchCommitted(
                        new Partition(coordinator.config().topic(), Integer.parseInt(parts[1])));
                case "STATE" -> handleState();
                case "VERIFY" -> "OK " + coordinator.verifyNoOwnershipOverlap();
                default -> "ERR UNKNOWN_MEMBER unknown request: " + parts[0];
            };
        } catch (CoordinatorException e) {
            return "ERR " + e.reason().name() + " " + e.getMessage();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "ERR REBALANCE_IN_PROGRESS interrupted while waiting";
        } catch (RuntimeException e) {
            return "ERR UNKNOWN_MEMBER bad request: " + e.getMessage();
        }
    }

    private HeartbeatResponse joinAwait(String memberId)
            throws CoordinatorException, InterruptedException {
        return coordinator.joinAndAwait(memberId);
    }

    private String handleConfig() {
        CoordinatorConfig c = coordinator.config();
        return "OK " + c.topic() + " " + c.partitionCount() + " " + c.heartbeatIntervalMs()
                + " " + c.sessionTimeoutMs() + " " + c.rebalanceTimeoutMs() + " "
                + c.rebalanceMode().name();
    }

    private String response(HeartbeatResponse r) {
        StringBuilder sb = new StringBuilder("OK ")
                .append(r.generation()).append(' ')
                .append(r.memberEpoch()).append(' ')
                .append(r.rebalanceInProgress() ? 1 : 0);
        for (Partition p : r.assignment()) {
            sb.append(' ').append(p.id());
        }
        return sb.toString();
    }

    private String handleState() {
        StringJoiner members = new StringJoiner(",");
        coordinator.members().forEach(members::add);
        StringJoiner owners = new StringJoiner(",");
        for (Map.Entry<Partition, String> e : coordinator.ownershipSnapshot().entrySet()) {
            owners.add(e.getKey().id() + "=" + e.getValue());
        }
        return "OK " + coordinator.state().name() + " " + coordinator.generation() + " "
                + (members.length() == 0 ? "-" : members) + " "
                + (owners.length() == 0 ? "-" : owners);
    }

    @Override
    public void close() {
        running = false;
        try {
            serverSocket.close();
        } catch (IOException ignored) {
        }
        for (Socket s : connections) {
            try {
                s.close();
            } catch (IOException ignored) {
            }
        }
    }
}
