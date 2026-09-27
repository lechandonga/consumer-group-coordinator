package com.github.lechandonga.cgc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Client/server wire protocol over loopback TCP: the same coordination
 * semantics as the embedded coordinator, exercised through real sockets.
 */
class NetworkedCoordinatorTest {

    @TempDir
    Path dir;

    private GroupCoordinator coordinator(CoordinatorConfig config) {
        return new GroupCoordinator(config);
    }

    @Test
    void joinHeartbeatCommitOverTcp() throws Exception {
        CoordinatorConfig config = new CoordinatorConfig(
                "topic", 4, 50, 500, 1000, dir.resolve("offsets.log"), RebalanceMode.BARRIER);
        try (GroupCoordinator gc = coordinator(config);
             CoordinatorServer server = new CoordinatorServer(gc, 0);
             CoordinatorClient c1 = new CoordinatorClient("127.0.0.1", server.port());
             CoordinatorClient c2 = new CoordinatorClient("127.0.0.1", server.port())) {

            HeartbeatResponse r1 = c1.joinAndAwait("m1");
            assertFalse(r1.rebalanceInProgress());
            assertEquals(4, r1.assignment().size());

            // Second member opens the barrier; m1 confirms, then both settle.
            HeartbeatResponse joining = c2.join("m2");
            assertTrue(joining.rebalanceInProgress(),
                    "the open barrier must be observable as in-progress");
            HeartbeatResponse r1b = c1.joinAndAwait("m1");
            HeartbeatResponse r2 = c2.joinAndAwait("m2");
            assertEquals(2, r2.assignment().size());
            assertEquals(2, r1b.assignment().size());
            assertEquals(GroupState.STABLE, gc.state());

            // Assignments cover everything and do not overlap.
            Set<Partition> all = new HashSet<>(r1b.assignment());
            all.addAll(r2.assignment());
            assertEquals(4, all.size());

            // Commit through the wire and read it back.
            Partition p = r2.assignment().iterator().next();
            c2.commit("m2", r2.memberEpoch(), r2.generation(), p, 7);
            assertEquals(7, c1.fetchCommitted(p));

            // Cross-member fencing is enforced remotely.
            CoordinatorException e = assertThrows(CoordinatorException.class,
                    () -> c1.commit("m1", r1b.memberEpoch(), r1b.generation(), p, 8));
            assertEquals(RejectionReason.NOT_OWNER, e.reason());

            // Epoch fencing over the wire: a new instance of m1 fences the old.
            HeartbeatResponse r1c = c2.joinAndAwait("m1");
            assertTrue(r1c.memberEpoch() > r1b.memberEpoch());
            CoordinatorException fenced = assertThrows(CoordinatorException.class,
                    () -> c1.heartbeat("m1", r1b.memberEpoch(), r1b.generation()));
            assertEquals(RejectionReason.FENCED_MEMBER_EPOCH, fenced.reason());

            assertTrue(c1.verifyNoOwnershipOverlap());
            assertTrue(c1.stateLine().startsWith("STABLE"));
        }
    }

    @Test
    void remoteMembersDriveFullCoverageAndProgress() throws Exception {
        CoordinatorConfig config = new CoordinatorConfig(
                "topic", 4, 50, 500, 1000, dir.resolve("offsets2.log"), RebalanceMode.BARRIER);
        try (GroupCoordinator gc = coordinator(config);
             CoordinatorServer server = new CoordinatorServer(gc, 0)) {
            Map<String, ConsumerMember> members = new HashMap<>();
            Map<String, CoordinatorClient> clients = new HashMap<>();
            for (String id : new String[]{"a", "b", "c"}) {
                CoordinatorClient client = new CoordinatorClient("127.0.0.1", server.port());
                ConsumerMember member = new ConsumerMember(id, client);
                member.start();
                clients.put(id, client);
                members.put(id, member);
            }
            // Wait until the group is stable and every partition is owned.
            long deadline = System.currentTimeMillis() + 10_000;
            while (true) {
                Set<Partition> covered = new HashSet<>();
                members.values().forEach(m -> covered.addAll(m.currentAssignment()));
                if (gc.state() == GroupState.STABLE && covered.size() == 4) {
                    break;
                }
                if (System.currentTimeMillis() > deadline) {
                    throw new AssertionError("group did not stabilize: " + gc.state()
                            + " covered=" + covered.size());
                }
                Thread.sleep(20);
            }
            // Everyone makes progress on its own partitions.
            for (int i = 0; i < 3; i++) {
                members.values().forEach(ConsumerMember::pollOnce);
            }
            for (int p = 0; p < 4; p++) {
                assertTrue(gc.fetchCommitted(new Partition("topic", p)) >= 0,
                        "partition " + p + " must be progressed by its remote owner");
            }
            assertTrue(gc.verifyNoOwnershipOverlap());
            members.values().forEach(ConsumerMember::shutdown);
            clients.values().forEach(CoordinatorClient::close);
        }
    }
}
