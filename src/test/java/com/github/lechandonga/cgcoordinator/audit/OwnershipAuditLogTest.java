package com.github.lechandonga.cgcoordinator.audit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OwnershipAuditLogTest {

    @Test
    void revokeBeforeGrantAcrossGenerationsHasNoOverlap() {
        OwnershipAuditLog log = new OwnershipAuditLog();
        log.grant(1, 0, null, "a", 1);
        log.revoke(1, 0, "a", null, 2);
        log.grant(2, 0, "a", "b", 3);
        assertFalse(log.hasSimultaneousOverlap());
    }

    @Test
    void grantToSecondOwnerWhileFirstStillActiveIsOverlap() {
        OwnershipAuditLog log = new OwnershipAuditLog();
        log.grant(1, 0, null, "a", 1);
        log.grant(1, 0, "a", "b", 2); // a still active in same generation
        assertTrue(log.hasSimultaneousOverlap());
    }

    @Test
    void sameOwnerReGrantIsNotOverlap() {
        OwnershipAuditLog log = new OwnershipAuditLog();
        log.grant(1, 0, null, "a", 1);
        log.grant(1, 0, "a", "a", 2);
        assertFalse(log.hasSimultaneousOverlap());
    }
}
