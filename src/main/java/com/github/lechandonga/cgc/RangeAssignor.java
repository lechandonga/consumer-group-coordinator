package com.github.lechandonga.cgc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Deterministic range assignor: partitions (sorted) are dealt to members
 * (sorted) in contiguous ranges. The result is a total function — every
 * partition is assigned to exactly one member and every member appears in the
 * result — so coverage and disjointness hold by construction.
 */
public class RangeAssignor implements Assignor {

    @Override
    public Map<String, Set<Partition>> assign(List<String> members, List<Partition> partitions) {
        List<String> sortedMembers = new ArrayList<>(members);
        sortedMembers.sort(String::compareTo);
        List<Partition> sortedPartitions = new ArrayList<>(partitions);
        sortedPartitions.sort(Partition::compareTo);

        Map<String, Set<Partition>> result = new LinkedHashMap<>();
        for (String member : sortedMembers) {
            result.put(member, new LinkedHashSet<>());
        }
        if (sortedMembers.isEmpty()) {
            return result;
        }
        int n = sortedPartitions.size();
        int m = sortedMembers.size();
        int base = n / m;
        int extra = n % m; // first `extra` members get one more partition
        int cursor = 0;
        for (int i = 0; i < m; i++) {
            int count = base + (i < extra ? 1 : 0);
            Set<Partition> owned = result.get(sortedMembers.get(i));
            for (int j = 0; j < count; j++) {
                owned.add(sortedPartitions.get(cursor++));
            }
        }
        return result;
    }
}
