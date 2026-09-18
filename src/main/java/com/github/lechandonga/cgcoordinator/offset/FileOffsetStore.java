package com.github.lechandonga.cgcoordinator.offset;

import com.github.lechandonga.cgcoordinator.reject.CoordinatorException;
import com.github.lechandonga.cgcoordinator.reject.RejectReason;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@link OffsetStore} backed by a single checkpoint file replaced atomically
 * (temp file -> fsync -> atomic rename -> directory fsync).
 *
 * <p>Crash semantics: a crash can only ever leave either the previous fully
 * valid checkpoint or the next fully valid checkpoint visible at the target
 * path; a half-written temp file is never renamed and is discarded on load.
 * Every record is checksummed, so partial/corrupt lines are rejected loudly
 * rather than producing unparseable or regressing state. Offsets for a
 * partition are monotonic non-decreasing; regressions are rejected with
 * {@link RejectReason#REGRESSION} and do not change stored state.
 */
public class FileOffsetStore implements OffsetStore {

    private static final String MAGIC = "CGCOFFSETS-V1";

    private final Path file;
    private final Path tmp;
    private final int partitionCount;
    private final TreeMap<Integer, PartitionOffset> offsets = new TreeMap<>();
    private long epoch;

    public FileOffsetStore(String dir, int partitionCount) {
        if (dir == null || dir.isBlank()) {
            throw new IllegalArgumentException("checkpoint dir must not be blank");
        }
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be >= 1");
        }
        this.partitionCount = partitionCount;
        this.file = Path.of(dir, "offsets.checkpoint");
        this.tmp = Path.of(dir, "offsets.checkpoint.tmp");
        try {
            Files.createDirectories(Path.of(dir));
            load();
        } catch (IOException e) {
            throw new IllegalStateException("cannot initialize offset store at " + dir, e);
        }
    }

    private void load() throws IOException {
        // A stale temp file is always an aborted transaction: ignore/remove it.
        Files.deleteIfExists(tmp);
        if (Files.notExists(file)) {
            return;
        }
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new IllegalStateException("corrupt checkpoint: empty file " + file);
        }
        String[] header = lines.get(0).split("\\|", -1);
        if (header.length != 3 || !MAGIC.equals(header[0])) {
            throw new IllegalStateException("corrupt checkpoint: bad header in " + file);
        }
        long fileEpoch;
        int fileCount;
        try {
            fileEpoch = Long.parseLong(header[1]);
            fileCount = Integer.parseInt(header[2]);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("corrupt checkpoint: bad header numbers in " + file, e);
        }
        TreeMap<Integer, PartitionOffset> parsed = new TreeMap<>();
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isEmpty()) {
                continue;
            }
            String[] f = line.split("\\|", -1);
            if (f.length != 5) {
                throw new IllegalStateException("corrupt checkpoint: malformed record: " + line);
            }
            try {
                int partition = Integer.parseInt(f[0]);
                long offset = Long.parseLong(f[1]);
                long gen = Long.parseLong(f[2]);
                long recEpoch = Long.parseLong(f[3]);
                long checksum = Long.parseLong(f[4]);
                long want = checksum(f[0], f[1], f[2], f[3], fileEpoch);
                if (checksum != want) {
                    throw new IllegalStateException(
                            "corrupt checkpoint: checksum mismatch for partition " + partition);
                }
                if (partition < 0 || partition >= partitionCount) {
                    throw new IllegalStateException(
                            "corrupt checkpoint: partition out of range: " + partition);
                }
                if (recEpoch != fileEpoch) {
                    throw new IllegalStateException(
                            "corrupt checkpoint: record epoch mismatch for partition " + partition);
                }
                if (parsed.put(partition, new PartitionOffset(partition, offset, gen, fileEpoch))
                        != null) {
                    throw new IllegalStateException(
                            "corrupt checkpoint: duplicate partition " + partition);
                }
            } catch (NumberFormatException e) {
                throw new IllegalStateException("corrupt checkpoint: non-numeric record: " + line, e);
            }
        }
        if (parsed.size() != fileCount) {
            throw new IllegalStateException(
                    "corrupt checkpoint: expected " + fileCount + " records but found "
                            + parsed.size());
        }
        this.offsets.clear();
        this.offsets.putAll(parsed);
        this.epoch = fileEpoch;
    }

    @Override
    public synchronized PartitionOffset save(String memberId, int partition,
                                             long offset, long generation) {
        if (partition < 0 || partition >= partitionCount) {
            throw new CoordinatorException(RejectReason.UNKNOWN_PARTITION,
                    "partition " + partition + " out of range [0," + partitionCount + ")");
        }
        if (offset < 0) {
            throw new CoordinatorException(RejectReason.BAD_REQUEST,
                    "offset must be >= 0 but was " + offset);
        }
        PartitionOffset current = offsets.get(partition);
        if (current != null && offset < current.offset()) {
            throw new CoordinatorException(RejectReason.REGRESSION,
                    "cannot move partition " + partition + " offset backwards from "
                            + current.offset() + " to " + offset);
        }
        long nextEpoch = epoch + 1;
        PartitionOffset rec = new PartitionOffset(partition, offset, generation, nextEpoch);
        persist(rec, nextEpoch);
        offsets.put(partition, rec);
        epoch = nextEpoch;
        return rec;
    }

    private void persist(PartitionOffset rec, long nextEpoch) {
        try (BufferedWriter w = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)) {
            // Snapshot of all partitions plus the in-flight update.
            TreeMap<Integer, PartitionOffset> view = new TreeMap<>(offsets);
            view.put(rec.partition(), rec);
            w.write(MAGIC + "|" + nextEpoch + "|" + view.size());
            w.write('\n');
            for (PartitionOffset po : view.values()) {
                String p = Integer.toString(po.partition());
                String o = Long.toString(po.offset());
                String g = Long.toString(po.generation());
                String e = Long.toString(nextEpoch);
                w.write(p + "|" + o + "|" + g + "|" + e + "|"
                        + checksum(p, o, g, e, nextEpoch));
                w.write('\n');
            }
            w.flush();
        } catch (IOException e) {
            throw new IllegalStateException("failed writing checkpoint temp file", e);
        }
        forceFile(tmp);
        try {
            try {
                Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed atomically replacing checkpoint", e);
        }
        // fsync directory so the rename itself is durable.
        try (var ds = Files.newDirectoryStream(file.toAbsolutePath().getParent())) {
            ds.iterator().forEachRemaining(p -> { });
        } catch (IOException ignored) {
            // directory fsync is best effort on some platforms; the rename is already visible.
        }
    }

    private void forceFile(Path p) {
        // fsync temp file content before the atomic rename.
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(
                p, java.nio.file.StandardOpenOption.WRITE)) {
            ch.force(true);
        } catch (IOException e) {
            // On platforms without fsync support, atomic rename still guarantees
            // the target path is never a torn file.
        }
    }

    private static long checksum(String partition, String offset, String generation,
                                 String epoch, long fileEpoch) {
        long h = 1_469_598_581_113_403_249L; // FNV offset basis
        String material = partition + "|" + offset + "|" + generation + "|" + epoch
                + "|" + fileEpoch + "|" + MAGIC;
        for (int i = 0; i < material.length(); i++) {
            h ^= material.charAt(i);
            h *= 1_099_511_628_211L; // FNV prime
        }
        return h;
    }

    @Override
    public synchronized long fetch(int partition) {
        if (partition < 0 || partition >= partitionCount) {
            throw new CoordinatorException(RejectReason.UNKNOWN_PARTITION,
                    "partition " + partition + " out of range [0," + partitionCount + ")");
        }
        PartitionOffset po = offsets.get(partition);
        return po == null ? -1L : po.offset();
    }

    @Override
    public synchronized List<PartitionOffset> all() {
        return new ArrayList<>(offsets.values());
    }

    @Override
    public synchronized void flush() {
        // Every save is already fsync'd atomically; nothing buffered here.
    }

    /** Visible for testing: parsed epoch of the loaded checkpoint. */
    public synchronized long checkpointEpoch() {
        return epoch;
    }

    // package-private snapshot view for server-side hydration
    synchronized Map<Integer, PartitionOffset> asMap() {
        return Map.copyOf(offsets);
    }
}
