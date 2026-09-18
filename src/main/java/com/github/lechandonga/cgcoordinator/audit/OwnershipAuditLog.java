package com.github.lechandonga.cgcoordinator.audit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Append-only ownership transition history.
 *
 * <p>The coordinator emits a GRANT when a partition ownership becomes
 * established in a new generation and a REVOKE at the instant the previous
 * generation is torn down. Events are ordered by a monotonically increasing
 * {@code epoch}, and the auditor verifies that no partition has two active
 * owners within the same generation simultaneously. This makes the temporary
 * overlap window during rebalance both bounded and machine-detectable.
 */
public class OwnershipAuditLog {

    /** Kind of ownership transition. */
    public enum Kind {
        GRANT,
        REVOKE
    }

    private final List<OwnershipEvent> events = new ArrayList<>();
    private long epochSeq;

    public synchronized long grant(long generation, int partition,
                                   String oldOwner, String newOwner, long timestampMillis) {
        return append(Kind.GRANT, generation, partition, oldOwner, newOwner, timestampMillis);
    }

    public synchronized long revoke(long generation, int partition,
                                    String oldOwner, String newOwner, long timestampMillis) {
        return append(Kind.REVOKE, generation, partition, oldOwner, newOwner, timestampMillis);
    }

    private long append(Kind kind, long generation, int partition,
                        String oldOwner, String newOwner, long timestampMillis) {
        long epoch = ++epochSeq;
        events.add(new OwnershipEvent(epoch, kind, generation, partition,
                oldOwner, newOwner, timestampMillis));
        return epoch;
    }

    public synchronized List<OwnershipEvent> events() {
        return List.copyOf(events);
    }

    /**
     * Detects simultaneous duplicate ownership: within a single generation,
     * a GRANT of partition P to owner B occurs while an earlier GRANT to a
     * different owner A has not yet been REVOKEd. Under the coordinator's
     * single-lock revoke-before-grant protocol this must never be true.
     */
    public synchronized boolean hasSimultaneousOverlap() {
        // key: generation -> partition -> currently active owner
        Map<Long, Map<Integer, String>> active = new TreeMap<>();
        for (OwnershipEvent e : events) {
            Map<Integer, String> parts = active.computeIfAbsent(e.generation(), g -> new TreeMap<>());
            if (e.kind() == Kind.GRANT) {
                String current = parts.get(e.partition());
                if (current != null && !current.equals(e.newOwner())) {
                    return true;
                }
                parts.put(e.partition(), e.newOwner());
            } else {
                String current = parts.get(e.partition());
                if (current != null && e.oldOwner() != null
                        && current.equals(e.oldOwner())) {
                    parts.remove(e.partition());
                }
            }
        }
        return false;
    }

    /** Events affecting one partition, in epoch order. */
    public synchronized List<OwnershipEvent> forPartition(int partition) {
        return events.stream().filter(e -> e.partition() == partition).toList();
    }
}
