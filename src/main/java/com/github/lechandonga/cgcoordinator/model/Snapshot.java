package com.github.lechandonga.cgcoordinator.model;

import java.util.List;
import java.util.Map;

/**
 * Point-in-time membership/assignment view of the group.
 */
public record Snapshot(long generation, List<String> members,
                       Map<String, List<Integer>> assignments, String state) {
    public Snapshot {
        members = List.copyOf(members);
        assignments = Map.copyOf(assignments);
    }
}
