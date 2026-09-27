package com.github.lechandonga.cgc;

import java.util.Set;

/**
 * Result of {@code joinGroup} / {@code syncGroup}.
 *
 * @param status               STABLE when the generation closed, IN_PROGRESS while open
 * @param generation           generation the caller should carry on subsequent requests
 * @param instanceToken        opaque instance identity issued to / used by the caller
 * @param assignment           partitions owned by the member; empty while IN_PROGRESS
 * @param completedByTimeout   true when STABLE was reached because the rebalance
 *                             time bound elapsed (deterministic close), not because
 *                             every expected member rejoined in time
 */
public record JoinResult(JoinStatus status, int generation, String instanceToken,
                         Set<Partition> assignment, boolean completedByTimeout) {

    public JoinResult {
        assignment = Set.copyOf(assignment);
    }

    public static JoinResult inProgress(int generation, String instanceToken) {
        return new JoinResult(JoinStatus.IN_PROGRESS, generation, instanceToken, Set.of(), false);
    }
}
