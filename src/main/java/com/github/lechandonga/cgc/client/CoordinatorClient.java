package com.github.lechandonga.cgc.client;

import com.github.lechandonga.cgc.CoordinatorException;
import com.github.lechandonga.cgc.HeartbeatResponse;
import com.github.lechandonga.cgc.JoinResult;
import com.github.lechandonga.cgc.Partition;
import java.io.Closeable;
import java.io.IOException;

/**
 * Client for {@link com.github.lechandonga.cgc.server.CoordinatorServer}.
 * One instance per consumer process; holds a single persistent TCP connection.
 *
 * <p>Protocol-level rejections arrive as {@link CoordinatorException} with the
 * same {@link com.github.lechandonga.cgc.RejectionReason} values used embedded;
 * transport problems arrive as {@link IOException}.
 */
public final class CoordinatorClient implements Closeable {

    public CoordinatorClient(String host, int port) throws IOException {
    }

    /**
     * Phase 1/2 of a join. {@code instanceToken} null/empty means "I am a fresh
     * instance of this member id"; the returned token must be presented on
     * every subsequent request for this instance.
     */
    public JoinResult joinGroup(String memberId, String instanceToken)
            throws IOException, CoordinatorException {
        return null;
    }

    /** Phase 2/2: blocks (up to the server-side bound) until the generation closes. */
    public JoinResult syncGroup(String memberId, String instanceToken, int generation)
            throws IOException, CoordinatorException {
        return null;
    }

    public HeartbeatResponse heartbeat(String memberId, String instanceToken, int generation)
            throws IOException, CoordinatorException {
        return null;
    }

    public void commit(String memberId, String instanceToken, int generation,
                       Partition partition, long offset)
            throws IOException, CoordinatorException {
    }

    public void leave(String memberId, String instanceToken) throws IOException {
    }

    public long fetchCommitted(Partition partition) throws IOException {
        return -1L;
    }

    @Override
    public void close() throws IOException {
    }
}
