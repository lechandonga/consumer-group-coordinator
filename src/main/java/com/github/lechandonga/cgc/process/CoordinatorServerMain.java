package com.github.lechandonga.cgc.process;

/**
 * Entry point: runs the coordinator as an independent process.
 *
 * <pre>
 *   java ... CoordinatorServerMain &lt;port(0=ephemeral)&gt; &lt;partitionCount&gt;
 *        &lt;heartbeatIntervalMs&gt; &lt;sessionTimeoutMs&gt; &lt;rebalanceTimeoutMs&gt;
 *        &lt;offsetStorePath&gt;
 * </pre>
 *
 * The bound port is printed to stdout as {@code BOUND &lt;port&gt;}; the process
 * serves until killed.
 */
public final class CoordinatorServerMain {

    public static void main(String[] args) throws Exception {
    }

    private CoordinatorServerMain() {
    }
}
