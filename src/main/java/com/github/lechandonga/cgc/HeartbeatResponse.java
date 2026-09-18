package com.github.lechandonga.cgc;

import java.util.Set;

/** Result of a successful heartbeat: current generation and this member's assignment. */
public record HeartbeatResponse(int generation, Set<Partition> assignment) {
    public HeartbeatResponse {
        assignment = Set.copyOf(assignment);
    }
}
