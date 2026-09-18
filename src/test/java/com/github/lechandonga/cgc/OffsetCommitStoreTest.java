package com.github.lechandonga.cgc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OffsetCommitStoreTest {

    @TempDir
    Path dir;

    private Path storeFile() {
        return dir.resolve("offsets.log");
    }

    @Test
    void commitsSurviveReopen() {
        Partition p0 = new Partition("topic", 0);
        Partition p1 = new Partition("topic", 1);
        try (OffsetCommitStore store = new OffsetCommitStore(storeFile())) {
            store.commit(p0, 10);
            store.commit(p1, 20);
            store.commit(p0, 11);
        }
        try (OffsetCommitStore store = new OffsetCommitStore(storeFile())) {
            assertEquals(11, store.committed(p0));
            assertEquals(20, store.committed(p1));
            assertEquals(-1, store.committed(new Partition("topic", 2)));
        }
    }

    @Test
    void offsetRegressionRejected() {
        Partition p0 = new Partition("topic", 0);
        try (OffsetCommitStore store = new OffsetCommitStore(storeFile())) {
            store.commit(p0, 10);
            assertThrows(IllegalArgumentException.class, () -> store.commit(p0, 9));
            assertEquals(10, store.committed(p0));
        }
    }

    @Test
    void tornTailIsTruncatedOnRecovery() throws Exception {
        Partition p0 = new Partition("topic", 0);
        try (OffsetCommitStore store = new OffsetCommitStore(storeFile())) {
            store.commit(p0, 5);
            store.commit(p0, 6);
        }
        // Simulate a crash mid-write: append garbage bytes (half a frame).
        long sizeBefore = Files.size(storeFile());
        try (RandomAccessFile raf = new RandomAccessFile(storeFile().toFile(), "rw")) {
            raf.seek(raf.length());
            raf.writeInt(100); // claims a 100-byte payload that never arrives
            raf.write(new byte[]{1, 2, 3});
        }
        try (OffsetCommitStore store = new OffsetCommitStore(storeFile())) {
            // Recovery sees the last fully confirmed commit, nothing unparsable.
            assertEquals(6, store.committed(p0));
            // Torn tail was truncated.
            assertEquals(sizeBefore, Files.size(storeFile()));
            // And the store keeps working afterwards.
            store.commit(p0, 7);
            assertEquals(7, store.committed(p0));
        }
        try (OffsetCommitStore store = new OffsetCommitStore(storeFile())) {
            assertEquals(7, store.committed(p0));
        }
    }

    @Test
    void corruptedPayloadIsDiscarded() throws Exception {
        Partition p0 = new Partition("topic", 0);
        try (OffsetCommitStore store = new OffsetCommitStore(storeFile())) {
            store.commit(p0, 3);
            store.commit(p0, 4);
        }
        // Flip a byte inside the last record's payload.
        try (RandomAccessFile raf = new RandomAccessFile(storeFile().toFile(), "rw")) {
            long len = raf.length();
            raf.seek(len - 2);
            int b = raf.read();
            raf.seek(len - 2);
            raf.write(b ^ 0xFF);
        }
        try (OffsetCommitStore store = new OffsetCommitStore(storeFile())) {
            // CRC mismatch -> last record discarded, previous state recovered.
            assertEquals(3, store.committed(p0));
        }
    }
}
