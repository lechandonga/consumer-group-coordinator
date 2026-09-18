package com.github.lechandonga.cgcoordinator.assign;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deterministic assignor: members are sorted lexicographically and
 * partitions are dealt round-robin, so partition {@code p} goes to the
 * member at index {@code p mod memberCount}. The resulting assignment is a
 * complete cover of {@code [0, partitionCount)} and partitions are pairwise
 * disjoint across members.
 */
public class RangeAssignor implements PartitionAssignor {

    @Override
    public Map<String, List<Integer>> assign(List<String> members, int partitionCount) {
        if (partitionCount < 0) {
            throw new IllegalArgumentException("partitionCount must be >= 0");
        }
        Map<String, List<Integer>> result = new LinkedHashMap<>();
        if (members == null || members.isEmpty()) {
            return result;
        }
        List<String> sorted = members.stream().sorted().distinct().toList();
        List<List<Integer>> buckets = new ArrayList<>(sorted.size());
        for (int i = 0; i < sorted.size(); i++) {
            buckets.add(new ArrayList<>());
        }
        for (int p = 0; p < partitionCount; p++) {
            buckets.get(p % sorted.size()).add(p);
        }
        for (int i = 0; i < sorted.size(); i++) {
            result.put(sorted.get(i), List.copyOf(buckets.get(i)));
        }
        return result;
    }
}
