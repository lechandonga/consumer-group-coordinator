package com.github.lechandonga.cgcoordinator.client;

import com.github.lechandonga.cgcoordinator.model.CommitResult;

import java.util.List;

/**
 * Local-process consumer: joins group, heartbeats, consumes assigned
 * partitions and commits offsets (stub).
 */
public class ConsumerClient implements AutoCloseable {

    public ConsumerClient(String serverHost, int serverPort, String memberId) {
        // stub
    }

    public List<Integer> join(long timeoutMillis) {
        throw new UnsupportedOperationException("stub");
    }

    public void startHeartbeat(long intervalMillis) {
        // stub
    }

    public CommitResult commit(int partition, long offset) {
        throw new UnsupportedOperationException("stub");
    }

    public long fetchOffset(int partition) {
        throw new UnsupportedOperationException("stub");
    }

    public void leave() {
        // stub
    }

    public long generation() {
        throw new UnsupportedOperationException("stub");
    }

    public List<Integer> partitions() {
        throw new UnsupportedOperationException("stub");
    }

    @Override
    public void close() {
        // stub
    }
}
