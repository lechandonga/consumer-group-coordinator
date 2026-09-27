package com.github.lechandonga.cgc.process;

/**
 * Entry point: a real consumer process joining a coordinator over TCP.
 *
 * <pre>
 *   java ... ConsumerProcessMain &lt;host&gt; &lt;port&gt; &lt;memberId&gt; [&lt;instanceToken&gt;]
 *        [--duration-ms N] [--slow-join]
 * </pre>
 *
 * <p>Without an instance token the process registers as a <em>new instance</em>
 * (fencing any previous instance of the same member id) and prints
 * {@code TOKEN &lt;token&gt;}. All lifecycle events (join, assignment, commits,
 * rejections with reasons, takeover) are printed one per line, making the
 * "old instance rejected / new instance takes over" result directly
 * observable. {@code --slow-join} joins (phase 1) but never syncs, which lets
 * tests exercise the rebalance time bound.
 */
public final class ConsumerProcessMain {

    public static void main(String[] args) throws Exception {
    }

    private ConsumerProcessMain() {
    }
}
