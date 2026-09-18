package com.github.lechandonga.cgcoordinator.net;

import com.github.lechandonga.cgcoordinator.group.GroupCoordinator;

import java.net.ServerSocket;

/** Minimal length-prefixed TCP server fronting a {@link GroupCoordinator} (stub). */
public class CoordinatorServer implements AutoCloseable {

    public CoordinatorServer(GroupCoordinator coordinator, int port) {
        // stub
    }

    public void start() {
        // stub
    }

    public int boundPort() {
        throw new UnsupportedOperationException("stub");
    }

    public ServerSocket serverSocket() {
        throw new UnsupportedOperationException("stub");
    }

    @Override
    public void close() {
        // stub
    }
}
