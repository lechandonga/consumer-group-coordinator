package com.github.lechandonga.cgcoordinator.offset;

/**
 * Durable offset record for one partition.
 *
 * @param partition partition id
 * @param offset    next offset to consume (monotonic non-decreasing)
 * @param generation generation in which the offset was committed
 * @param epoch     checkpoint-file record version, bumping on every durable write
 */
public record PartitionOffset(int partition, long offset, long generation, long epoch) {
}
