package com.github.lechandonga.cgc;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Append-only durable store of committed offsets.
 *
 * <p>On-disk layout: an 8-byte magic header followed by framed records
 * {@code [len:int][crc32:long][payload]}, where payload is
 * {@code [topicLen:short][topic][partition:int][offset:long]}.
 *
 * <p>Recovery scans frames in order and stops at the first incomplete or
 * CRC-mismatched frame (a torn tail left by a crash mid-write), truncating the
 * file at that point. The recovered state therefore always equals the last
 * fully confirmed commit: never unparsable, never regressed.
 */
public class OffsetCommitStore implements Closeable {

    private static final long MAGIC = 0x43474F4646535431L; // "CGOFFST1"
    private static final int HEADER_LEN = Long.BYTES;
    private static final int FRAME_HEADER_LEN = Integer.BYTES + Long.BYTES;

    private final Path file;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final Map<Partition, Long> offsets = new LinkedHashMap<>();
    private boolean closed;

    public OffsetCommitStore(Path file) {
        this.file = file;
        try {
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            boolean exists = Files.exists(file) && Files.size(file) > 0;
            this.raf = new RandomAccessFile(file.toFile(), "rw");
            this.channel = raf.getChannel();
            if (!exists) {
                raf.writeLong(MAGIC);
                channel.force(true);
            } else {
                recover();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open offset store " + file, e);
        }
    }

    private void recover() throws IOException {
        raf.seek(0);
        long magic = raf.readLong();
        if (magic != MAGIC) {
            throw new IOException("bad magic in offset store " + file + ": not a valid store file");
        }
        long validEnd = HEADER_LEN;
        long length = raf.length();
        while (validEnd + FRAME_HEADER_LEN <= length) {
            raf.seek(validEnd);
            int len = raf.readInt();
            long crc = raf.readLong();
            if (len <= 0 || validEnd + FRAME_HEADER_LEN + len > length) {
                break; // torn frame header or truncated payload
            }
            byte[] payload = new byte[len];
            raf.readFully(payload);
            CRC32 crc32 = new CRC32();
            crc32.update(payload);
            if (crc32.getValue() != crc) {
                break; // corrupt payload: treat as torn tail
            }
            apply(payload);
            validEnd += FRAME_HEADER_LEN + len;
        }
        // Truncate any torn/corrupt tail so future appends start from a clean point.
        if (validEnd < length) {
            channel.truncate(validEnd);
        }
        raf.seek(validEnd);
    }

    private void apply(byte[] payload) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        short topicLen = buf.getShort();
        byte[] topicBytes = new byte[topicLen];
        buf.get(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.UTF_8);
        int partitionId = buf.getInt();
        long offset = buf.getLong();
        offsets.put(new Partition(topic, partitionId), offset);
    }

    /** Durably records offset for partition. Offsets must be monotonic per partition. */
    public synchronized void commit(Partition partition, long offset) {
        ensureOpen();
        Long previous = offsets.get(partition);
        if (previous != null && offset < previous) {
            throw new IllegalArgumentException(
                    "offset regression for " + partition + ": " + offset + " < " + previous);
        }
        byte[] topicBytes = partition.topic().getBytes(StandardCharsets.UTF_8);
        ByteBuffer payload = ByteBuffer.allocate(Short.BYTES + topicBytes.length + Integer.BYTES + Long.BYTES);
        payload.putShort((short) topicBytes.length);
        payload.put(topicBytes);
        payload.putInt(partition.id());
        payload.putLong(offset);
        byte[] payloadBytes = payload.array();
        CRC32 crc32 = new CRC32();
        crc32.update(payloadBytes);
        try {
            ByteBuffer frame = ByteBuffer.allocate(FRAME_HEADER_LEN + payloadBytes.length);
            frame.putInt(payloadBytes.length);
            frame.putLong(crc32.getValue());
            frame.put(payloadBytes);
            frame.flip();
            channel.write(frame);
            channel.force(false); // fsync before the commit is considered confirmed
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append commit to " + file, e);
        }
        offsets.put(partition, offset);
    }

    /** Last durably committed offset for partition, or -1 if none. */
    public synchronized long committed(Partition partition) {
        return offsets.getOrDefault(partition, -1L);
    }

    /** Snapshot of all committed offsets. */
    public synchronized Map<Partition, Long> snapshot() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(offsets));
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("offset store is closed");
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            channel.force(false);
            raf.close();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to close offset store " + file, e);
        }
    }

    @Override
    public String toString() {
        return "OffsetCommitStore{" + file + "}";
    }
}
