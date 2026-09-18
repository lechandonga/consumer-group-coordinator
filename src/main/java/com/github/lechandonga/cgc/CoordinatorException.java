package com.github.lechandonga.cgc;

/** Exception carrying a machine-distinguishable {@link RejectionReason}. */
public class CoordinatorException extends Exception {
    private final RejectionReason reason;

    public CoordinatorException(RejectionReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectionReason reason() {
        return reason;
    }
}
