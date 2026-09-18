package com.github.lechandonga.cgc;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Map;

/**
 * Append-only durable store of committed offsets. Torn or half-written tails
 * from a crash are detected via CRC framing and truncated on recovery, so a
 * restart always resumes from the last fully confirmed commit.
 */
public class OffsetCommitStore implements Closeable {

    public OffsetCommitStore(Path file) {
        throw new UnsupportedOperationException("skeleton");
    }

    /** Durably records offset for partition. Must be monotonic per partition. */
    public synchronized void commit(Partition partition, long offset) {
        throw new UnsupportedOperationException("skeleton");
    }

    /** Last durably committed offset for partition, or -1 if none. */
    public synchronized long committed(Partition partition) {
        throw new UnsupportedOperationException("skeleton");
    }

    /** Snapshot of all committed offsets. */
    public synchronized Map<Partition, Long> snapshot() {
        throw new UnsupportedOperationException("skeleton");
    }

    @Override
    public synchronized void close() {
        throw new UnsupportedOperationException("skeleton");
    }
}
