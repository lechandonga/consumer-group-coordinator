package com.github.lechandonga.cgc;

import java.util.Objects;

/** A single topic partition. */
public record Partition(String topic, int id) implements Comparable<Partition> {
    public Partition {
        Objects.requireNonNull(topic, "topic");
        if (id < 0) throw new IllegalArgumentException("partition id must be >= 0");
    }

    @Override
    public int compareTo(Partition other) {
        int c = topic.compareTo(other.topic);
        return c != 0 ? c : Integer.compare(id, other.id);
    }

    @Override
    public String toString() {
        return topic + "-" + id;
    }
}
