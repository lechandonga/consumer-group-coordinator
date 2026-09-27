package com.github.lechandonga.cgc;

import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;

/**
 * Standalone consumer process. Usage:
 * <pre>
 *   java ... ConsumerMemberMain --host 127.0.0.1 --port 9000 --id m1 [--max-runtime-ms 0]
 * </pre>
 * Emits machine-parseable events on stdout, one per line:
 * <pre>
 *   READY id=&lt;id&gt;
 *   ASSIGNED id=&lt;id&gt; gen=&lt;g&gt; epoch=&lt;e&gt; parts=[0,2]
 *   POSITIONS id=&lt;id&gt; 0=3,2=5        (only on change)
 *   FENCED id=&lt;id&gt; rejections=&lt;n&gt;  (this instance was replaced; process exits)
 *   STOPPED id=&lt;id&gt;
 * </pre>
 */
public final class ConsumerMemberMain {

    private ConsumerMemberMain() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = CoordinatorServerMain.parse(args);
        String host = opts.getOrDefault("host", "127.0.0.1");
        int port = Integer.parseInt(opts.get("port"));
        String id = opts.get("id");
        long maxRuntimeMs = Long.parseLong(opts.getOrDefault("max-runtime-ms", "0"));

        CoordinatorClient client = new CoordinatorClient(host, port);
        ConsumerMember member = new ConsumerMember(id, client);
        member.start();
        System.out.println("READY id=" + id);

        long pollMs = Math.max(20, client.config().heartbeatIntervalMs());
        long deadline = maxRuntimeMs > 0 ? System.currentTimeMillis() + maxRuntimeMs : Long.MAX_VALUE;
        String lastAssignment = "";
        String lastPositions = "";
        while (System.currentTimeMillis() < deadline && !member.isFenced()) {
            member.pollOnce();
            String assignment = assignmentLine(id, member);
            if (!assignment.equals(lastAssignment)) {
                System.out.println(assignment);
                lastAssignment = assignment;
            }
            String positions = positionsLine(id, member);
            if (!positions.equals(lastPositions)) {
                System.out.println(positions);
                lastPositions = positions;
            }
            Thread.sleep(pollMs);
        }
        if (member.isFenced()) {
            System.out.println("FENCED id=" + id + " rejections=" + member.commitRejections());
        } else {
            member.shutdown();
            System.out.println("STOPPED id=" + id);
        }
        client.close();
    }

    private static String assignmentLine(String id, ConsumerMember member) {
        Set<Partition> parts = member.currentAssignment();
        StringJoiner joiner = new StringJoiner(",", "[", "]");
        parts.stream().mapToInt(Partition::id).sorted().forEach(p -> joiner.add(String.valueOf(p)));
        return "ASSIGNED id=" + id + " parts=" + joiner;
    }

    private static String positionsLine(String id, ConsumerMember member) {
        Map<Partition, Long> sorted = new TreeMap<>(member.localPositions());
        StringJoiner joiner = new StringJoiner(",");
        sorted.forEach((p, o) -> joiner.add(p.id() + "=" + o));
        return "POSITIONS id=" + id + " " + joiner;
    }
}
