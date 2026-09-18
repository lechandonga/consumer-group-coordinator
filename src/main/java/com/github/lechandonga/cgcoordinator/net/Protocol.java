package com.github.lechandonga.cgcoordinator.net;

/**
 * Line-based request/response protocol. One request is one pipe-delimited
 * text line; the server returns one pipe-delimited response line. Keeping it
 * textual makes requests/responses directly observable and reproducible.
 *
 * <h2>Requests</h2>
 * <pre>
 *   JOIN|memberId|incarnationId|timeoutMillis
 *   HB  |memberId|incarnationId|generation
 *   LEAVE|memberId|incarnationId|generation
 *   COMMIT|memberId|incarnationId|generation|partition|offset
 *   FETCH|partition
 *   SNAP
 * </pre>
 *
 * <h2>Responses</h2>
 * <pre>
 *   OK|generation|state|p1,p2,...            (JOIN, empty list allowed)
 *   OK|generation|state                      (HB, LEAVE)
 *   OK|generation|partition|offset           (COMMIT)
 *   OK|offset                                (FETCH; -1 = unknown)
 *   OK|generation|state|member:parts;...     (SNAP)
 *   ERR|REASON|message                       (any rejected request)
 * </pre>
 *
 * Rejection reasons carry the {@code RejectReason} enum name verbatim, so a
 * client can distinguish stale generation, fenced incarnation, non-owner,
 * rebalancing, regression, etc.
 */
public final class Protocol {

    public static final String DELIM = "|";
    public static final String OK = "OK";
    public static final String ERR = "ERR";

    public static final String REQ_JOIN = "JOIN";
    public static final String REQ_HB = "HB";
    public static final String REQ_LEAVE = "LEAVE";
    public static final String REQ_COMMIT = "COMMIT";
    public static final String REQ_FETCH = "FETCH";
    public static final String REQ_SNAP = "SNAP";

    private Protocol() {
    }

    public static String ok(String... fields) {
        StringBuilder sb = new StringBuilder(OK);
        for (String f : fields) {
            sb.append(DELIM).append(f);
        }
        return sb.toString();
    }

    public static String err(String reason, String message) {
        return ERR + DELIM + reason + DELIM + (message == null ? "" : message.replace('|', '/'));
    }
}
