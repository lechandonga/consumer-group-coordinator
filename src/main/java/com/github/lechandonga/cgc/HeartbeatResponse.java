package com.github.lechandonga.cgc;

import java.util.Set;

/**
 * Result of a successful join/heartbeat: current generation, this member
 * instance's fencing epoch, and this member's assignment.
 *
 * @param generation current group generation
 * @param memberEpoch fencing epoch identifying this member instance; a join by
 *                    a newer instance of the same member id invalidates all
 *                    lower epochs
 * @param assignment partitions owned in the current generation (empty while a
 *                   rebalance barrier is open)
 * @param rebalanceInProgress true while a rebalance barrier is open: the
 *                            assignment is not final yet and the member must
 *                            re-join to confirm and learn the outcome
 */
public record HeartbeatResponse(int generation, long memberEpoch, Set<Partition> assignment,
                                boolean rebalanceInProgress) {
    public HeartbeatResponse {
        assignment = Set.copyOf(assignment);
    }
}
