package com.github.lechandonga.cgc;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** Manually advanced clock for deterministic tests. */
final class MutableClock implements LongSupplier {
    private final AtomicLong now;

    MutableClock(long start) {
        this.now = new AtomicLong(start);
    }

    @Override
    public long getAsLong() {
        return now.get();
    }

    void advance(long ms) {
        now.addAndGet(ms);
    }
}
