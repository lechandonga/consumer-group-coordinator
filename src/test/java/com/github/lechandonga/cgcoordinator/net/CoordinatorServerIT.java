package com.github.lechandonga.cgcoordinator.net;

import com.github.lechandonga.cgcoordinator.assign.RangeAssignor;
import com.github.lechandonga.cgcoordinator.audit.OwnershipAuditLog;
import com.github.lechandonga.cgcoordinator.config.CoordinatorConfig;
import com.github.lechandonga.cgcoordinator.group.GroupCoordinator;
import com.github.lechandonga.cgcoordinator.offset.FileOffsetStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** White-box protocol tests over a real loopback TCP socket. */
class CoordinatorServerIT {

    @TempDir
    Path dir;

    private CoordinatorServer server;
    private int port;

    @BeforeEach
    void start() {
        CoordinatorConfig cfg = new CoordinatorConfig(6, 5_000, 1_000, 1_000, dir.toString());
        GroupCoordinator c = new GroupCoordinator(cfg, new RangeAssignor(),
                new FileOffsetStore(dir.toString(), 6), new OwnershipAuditLog());
        server = new CoordinatorServer(c, 0, 50);
        server.start();
        port = server.boundPort();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private String[] roundTrip(String req) throws Exception {
        try (Socket s = new Socket("127.0.0.1", port);
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter out = new BufferedWriter(
                     new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8))) {
            out.write(req);
            out.write('\n');
            out.flush();
            return in.readLine().split("\\|", -1);
        }
    }

    @Test
    void joinThenHeartbeatThenCommitFlow() throws Exception {
        String[] join = roundTrip("JOIN|a|a-1|2000");
        assertEquals("OK", join[0]);
        long gen = Long.parseLong(join[1]);
        assertEquals("STABLE", join[2]);
        assertEquals("0,1,2,3,4,5", join[3]);

        String[] hb = roundTrip("HB|a|a-1|" + gen);
        assertEquals("OK", hb[0]);

        String[] commit = roundTrip("COMMIT|a|a-1|" + gen + "|2|42");
        assertEquals("OK", commit[0]);
        assertEquals("42", commit[3]);

        String[] fetch = roundTrip("FETCH|2");
        assertEquals("OK", fetch[0]);
        assertEquals("42", fetch[1]);
    }

    @Test
    void staleGenerationCommitReturnsDistinguishableReason() throws Exception {
        String[] join = roundTrip("JOIN|a|a-1|2000");
        long gen = Long.parseLong(join[1]);
        // advance generation with a second member and rejoin a concurrently
        try (Socket s = new Socket("127.0.0.1", port)) {
            var in = new BufferedReader(new InputStreamReader(
                    s.getInputStream(), StandardCharsets.UTF_8));
            var out = new BufferedWriter(new OutputStreamWriter(
                    s.getOutputStream(), StandardCharsets.UTF_8));
            out.write("JOIN|b|b-1|2000\n");
            out.flush();
            in.readLine(); // b may be finalized into a later gen; ignore
        }
        // give reaper a moment, then re-stabilize a on its own new generation
        Thread.sleep(200);
        String[] rejoin = roundTrip("JOIN|a|a-1|2000");
        long newGen = Long.parseLong(rejoin[1]);
        assertTrue(newGen > gen);
        String[] stale = roundTrip("COMMIT|a|a-1|" + gen + "|0|1");
        assertEquals("ERR", stale[0]);
        assertEquals("STALE_GENERATION", stale[1]);
    }

    @Test
    void fencedIncarnationHeartbeatIsRejected() throws Exception {
        roundTrip("JOIN|a|a-1|2000");
        String[] rejoin = roundTrip("JOIN|a|a-2|2000");
        long gen = Long.parseLong(rejoin[1]);
        String[] zombie = roundTrip("HB|a|a-1|" + gen);
        assertEquals("ERR", zombie[0]);
        assertEquals("FENCED_INCARNATION", zombie[1]);
    }

    @Test
    void badCommandAndMalformedRequestsAreRejectedNotCrashingServer() throws Exception {
        String[] badCmd = roundTrip("BOGUS");
        assertEquals("ERR", badCmd[0]);
        assertEquals("BAD_REQUEST", badCmd[1]);
        String[] malformed = roundTrip("COMMIT|a|a-1|notanumber|0|1");
        assertEquals("ERR", malformed[0]);
        assertEquals("BAD_REQUEST", malformed[1]);
        // server still alive
        assertEquals("OK", roundTrip("SNAP")[0]);
    }

    @Test
    void unknownPartitionCommitRejected() throws Exception {
        String[] join = roundTrip("JOIN|a|a-1|2000");
        long gen = Long.parseLong(join[1]);
        String[] r = roundTrip("COMMIT|a|a-1|" + gen + "|99|1");
        assertEquals("ERR", r[0]);
        assertEquals("UNKNOWN_PARTITION", r[1]);
    }
}
