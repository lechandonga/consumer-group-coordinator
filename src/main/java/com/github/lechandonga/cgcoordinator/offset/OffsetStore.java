package com.github.lechandonga.cgcoordinator.offset;

import java.util.List;

/**
 * Durable per-partition offset store.
 * Implementations must guarantee crash-atomic, parseable, non-regressing state.
 */
public interface OffsetStore {

    /** Atomically persist one partition's offset; rejects regressions. */
    PartitionOffset save(String memberId, int partition, long offset, long generation);

    /** Read a single partition offset, or -1 if never committed. */
    long fetch(int partition);

    /** Read all known offsets. */
    List<PartitionOffset> all();

    /** Force durability barrier (fsync). */
    void flush();
}
