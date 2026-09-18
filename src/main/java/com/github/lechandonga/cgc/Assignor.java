package com.github.lechandonga.cgc;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Computes a complete, non-overlapping partition assignment for the given members. */
public interface Assignor {
    /**
     * @param members sorted member ids
     * @param partitions all partitions
     * @return assignment containing every member; union of values equals partitions, values disjoint
     */
    Map<String, Set<Partition>> assign(List<String> members, List<Partition> partitions);
}
