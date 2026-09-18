package com.github.lechandonga.cgcoordinator.offset;

import com.github.lechandonga.cgcoordinator.reject.CoordinatorException;
import com.github.lechandonga.cgcoordinator.reject.RejectReason;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileOffsetStoreTest {

    @TempDir
    Path dir;

    @Test
    void unknownPartitionOffsetsStartAtMinusOne() {
        FileOffsetStore store = new FileOffsetStore(dir.toString(), 4);
        assertEquals(-1L, store.fetch(0));
    }

    @Test
    void offsetsSurviveReopenAtConfirmedPosition() {
        FileOffsetStore store = new FileOffsetStore(dir.toString(), 4);
        store.save("a", 0, 10, 1);
        store.save("a", 2, 7, 1);

        FileOffsetStore reopened = new FileOffsetStore(dir.toString(), 4);
        assertEquals(10L, reopened.fetch(0));
        assertEquals(-1L, reopened.fetch(1));
        assertEquals(7L, reopened.fetch(2));
    }

    @Test
    void regressingOffsetIsRejectedAndDoesNotChangeState() {
        FileOffsetStore store = new FileOffsetStore(dir.toString(), 2);
        store.save("a", 1, 9, 1);
        CoordinatorException ex = assertThrows(CoordinatorException.class,
                () -> store.save("a", 1, 8, 1));
        assertEquals(RejectReason.REGRESSION, ex.reason());
        assertEquals(9L, store.fetch(1));

        // State on disk is still the committed (non-regressed) value.
        assertEquals(9L, new FileOffsetStore(dir.toString(), 2).fetch(1));
    }

    @Test
    void equalOffsetIsIdempotentAndAccepted() {
        FileOffsetStore store = new FileOffsetStore(dir.toString(), 2);
        store.save("a", 0, 5, 1);
        store.save("a", 0, 5, 2);
        assertEquals(5L, store.fetch(0));
    }

    @Test
    void staleTempFileFromCrashIsDiscarded() throws IOException {
        FileOffsetStore store = new FileOffsetStore(dir.toString(), 2);
        store.save("a", 0, 3, 1);
        Files.writeString(Path.of(dir.toString(), "offsets.checkpoint.tmp"),
                "garbage half-written bytes", StandardCharsets.UTF_8);

        FileOffsetStore reopened = new FileOffsetStore(dir.toString(), 2);
        assertEquals(3L, reopened.fetch(0));
        assertTrue(Files.notExists(Path.of(dir.toString(), "offsets.checkpoint.tmp")));
    }

    @Test
    void tornOrCorruptCheckpointIsRejectedLoudly() throws IOException {
        new FileOffsetStore(dir.toString(), 2).save("a", 0, 3, 1);
        Path file = Path.of(dir.toString(), "offsets.checkpoint");
        String content = Files.readString(file, StandardCharsets.UTF_8);
        // Flip a digit in the first data record's offset.
        String corrupted = content.replaceFirst("(?m)^0\\|3\\|", "0|4|");
        Files.writeString(file, corrupted, StandardCharsets.UTF_8);
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new FileOffsetStore(dir.toString(), 2));
        assertTrue(ex.getMessage().contains("corrupt checkpoint"));
    }

    @Test
    void partitionOutOfRangeIsRejected() {
        FileOffsetStore store = new FileOffsetStore(dir.toString(), 2);
        CoordinatorException ex = assertThrows(CoordinatorException.class,
                () -> store.save("a", 2, 1, 1));
        assertEquals(RejectReason.UNKNOWN_PARTITION, ex.reason());
    }
}
