package com.github.lechandonga.cgc.server;

import com.github.lechandonga.cgc.CoordinatorConfig;
import java.io.Closeable;

/**
 * TCP front-end exposing one {@code GroupCoordinator} to independent local
 * processes using a line-based text protocol (see README "多进程协议").
 *
 * <p>All requests from a connection are serialized; the coordinator itself is
 * thread-safe, so the generated assignment, generation fencing and instance
 * fencing behave exactly as in the embedded single-process case.
 */
public final class CoordinatorServer implements Closeable {

    public CoordinatorServer(CoordinatorConfig config, int port) {
    }

    /** Binds the listening socket and starts accepting; returns the bound port. */
    public int start() {
        return 0;
    }

    @Override
    public void close() {
    }
}
