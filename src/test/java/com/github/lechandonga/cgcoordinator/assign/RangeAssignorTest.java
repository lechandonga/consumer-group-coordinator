package com.github.lechandonga.cgcoordinator.assign;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RangeAssignorTest {

    private final RangeAssignor assignor = new RangeAssignor();

    @Test
    void assignmentCompletelyCoversAndIsDisjoint() {
        Map<String, List<Integer>> a = assignor.assign(List.of("c", "a", "b"), 7);
        Set<Integer> all = new HashSet<>();
        for (List<Integer> parts : a.values()) {
            for (int p : parts) {
                assertTrue(all.add(p), "partition " + p + " assigned twice");
            }
        }
        assertEquals(Set.of(0, 1, 2, 3, 4, 5, 6), all);
    }

    @Test
    void assignmentIsDeterministicRegardlessOfInputOrder() {
        Map<String, List<Integer>> a1 = assignor.assign(List.of("a", "b", "c"), 6);
        Map<String, List<Integer>> a2 = assignor.assign(List.of("c", "b", "a"), 6);
        assertEquals(a1, a2);
    }

    @Test
    void emptyMembersYieldsEmptyAssignment() {
        assertTrue(assignor.assign(List.of(), 4).isEmpty());
    }

    @Test
    void moreMembersThanPartitionsLeavesSomeEmpty() {
        Map<String, List<Integer>> a = assignor.assign(List.of("a", "b", "c"), 2);
        assertEquals(2, a.values().stream().mapToInt(List::size).sum());
    }
}
