package com.github.lechandonga.cgcoordinator.offset;

import java.util.List;

/**
 * OffsetStore backed by an atomic temp-file + rename checkpoint (stub).
 */
public class FileOffsetStore implements OffsetStore {

    public FileOffsetStore(String dir, int partitionCount) {
        // stub
    }

    @Override
    public PartitionOffset save(String memberId, int partition, long offset, long generation) {
        throw new UnsupportedOperationException("stub");
    }

    @Override
    public long fetch(int partition) {
        throw new UnsupportedOperationException("stub");
    }

    @Override
    public List<PartitionOffset> all() {
        throw new UnsupportedOperationException("stub");
    }

    @Override
    public void flush() {
        // stub
    }
}
