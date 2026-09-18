package com.github.lechandonga.cgcoordinator.audit;

import java.util.List;

/** Append-only ownership history with overlap detection (stub). */
public class OwnershipAuditLog {

    public void record(OwnershipEvent event) {
        // stub
    }

    public List<OwnershipEvent> events() {
        throw new UnsupportedOperationException("stub");
    }

    /** True if any two owners simultaneously held a partition within one active generation. */
    public boolean hasSimultaneousOverlap() {
        throw new UnsupportedOperationException("stub");
    }
}
