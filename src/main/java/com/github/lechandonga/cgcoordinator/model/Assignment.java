package com.github.lechandonga.cgcoordinator.model;

import java.util.List;

/**
 * Immutable assignment of partitions to one member within a generation.
 */
public record Assignment(long generation, String memberId, List<Integer> partitions) {
    public Assignment {
        partitions = List.copyOf(partitions);
    }
}
