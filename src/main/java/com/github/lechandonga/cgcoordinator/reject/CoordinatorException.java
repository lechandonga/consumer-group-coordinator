package com.github.lechandonga.cgcoordinator.reject;

/**
 * Exception carrying a machine-readable {@link RejectReason}.
 */
public class CoordinatorException extends RuntimeException {

    private final RejectReason reason;

    public CoordinatorException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
