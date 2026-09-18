package com.github.lechandonga.cgcoordinator.assign;

import java.util.List;
import java.util.Map;

/**
 * Computes a partition assignment for a member set.
 * Contract: partitions are a complete cover of [0..partitionCount), disjoint.
 */
public interface PartitionAssignor {

    Map<String, List<Integer>> assign(List<String> members, int partitionCount);
}
