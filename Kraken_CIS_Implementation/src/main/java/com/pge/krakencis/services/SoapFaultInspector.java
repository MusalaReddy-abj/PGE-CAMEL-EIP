package com.pge.krakencis.services;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility for detecting and classifying SOAP faults in HTTP response bodies.
 *
 * <p>SOAP services can return {@code HTTP 200 OK} with a fault element inside the
 * body — the HTTP layer reports success but the SOAP layer reports an error.
 * This class bridges that gap so the rest of the application can treat SOAP faults
 * the same way it treats HTTP error status codes.
 *
 * <h3>SOAP 1.1 fault codes</h3>
 * <ul>
 *   <li>{@code soap:Client} / {@code SOAP-ENV:Client} — client error (bad input) → DLQ</li>
 *   <li>{@code soap:Server} / {@code SOAP-ENV:Server} — server error (transient) → retry</li>
 * </ul>
 *
 * <h3>SOAP 1.2 fault codes</h3>
 * <ul>
 *   <li>{@code env:Sender}   → client error → DLQ</li>
 *   <li>{@code env:Receiver} → server error → retry</li>
 * </ul>
 *
 * <h3>Usage</h3>
 * <pre>
 * if (response.getStatusCode() == 200) {
 *     Integer inferred = SoapFaultInspector.inferHttpStatus(response.getBody());
 *     if (inferred != null) {
 *         // SOAP fault found — use inferred status for retry/DLQ routing
 *         statusCode = inferred;
 *     }
 * }
 * </pre>
 */
public final class SoapFaultInspector {

    // ── SOAP fault element markers (1.1 and 1.2, various namespace prefixes) ──
    private static final List<String> FAULT_ELEMENT_MARKERS = List.of(
        "<soap:Fault", "<env:Fault", "<soapenv:Fault",
        "<SOAP-ENV:Fault", "<s:Fault", "<Fault"
    );

    // ── Client / Sender fault code markers → permanent, DLQ ──────────────────
    private static final List<String> CLIENT_FAULT_MARKERS = List.of(
        "soap:Client",     "SOAP-ENV:Client",
        "env:Sender",      "soapenv:Client",
        ">Client<",        ">Client.Authentication<",
        ">Client.Authorization<"
    );

    // ── Server / Receiver fault code markers → transient, retry ──────────────
    private static final List<String> SERVER_FAULT_MARKERS = List.of(
        "soap:Server",    "SOAP-ENV:Server",
        "env:Receiver",   "soapenv:Server",
        ">Server<",       ">VersionMismatch<",
        ">MustUnderstand<"
    );

    // ── Fault message extraction — tries <faultstring>, <env:Text>, <reason> ─
    private static final Pattern FAULT_STRING_PATTERN = Pattern.compile(
        "<(?:[^:>]+:)?(?:faultstring|Text|Reason|reason)(?:[^>]*)>([^<]+)</",
        Pattern.CASE_INSENSITIVE);

    private SoapFaultInspector() {}

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Returns {@code true} if the response body contains a SOAP fault element.
     */
    public static boolean hasFault(String responseBody) {
        if (responseBody == null || responseBody.isBlank()) return false;
        return FAULT_ELEMENT_MARKERS.stream().anyMatch(responseBody::contains);
    }

    /**
     * Returns {@code true} if the SOAP fault is a client-side error
     * (bad input, authentication failure) — these are permanent and should go to DLQ.
     */
    public static boolean isClientFault(String responseBody) {
        if (!hasFault(responseBody)) return false;
        return CLIENT_FAULT_MARKERS.stream().anyMatch(responseBody::contains);
    }

    /**
     * Extracts the human-readable fault message from the SOAP body.
     * Returns {@code "Unknown SOAP fault"} if the message cannot be extracted.
     */
    public static String extractFaultMessage(String responseBody) {
        if (responseBody == null) return "Unknown SOAP fault";
        Matcher m = FAULT_STRING_PATTERN.matcher(responseBody);
        return m.find() ? m.group(1).trim() : "Unknown SOAP fault";
    }

    /**
     * Infers a virtual HTTP status code from the SOAP fault content so the existing
     * retry/DLQ routing logic (which is HTTP-status-based) works transparently.
     *
     * <table border="1">
     *   <tr><th>Condition</th><th>Virtual status</th><th>Routing outcome</th></tr>
     *   <tr><td>No fault found</td><td>{@code null}</td><td>Use real HTTP status</td></tr>
     *   <tr><td>Client / Sender fault</td><td>{@code 400}</td><td>DLQ — bad payload, no retry</td></tr>
     *   <tr><td>Server / Receiver fault</td><td>{@code 500}</td><td>Retry → retry queue</td></tr>
     *   <tr><td>Unknown fault code</td><td>{@code 500}</td><td>Retry (be safe)</td></tr>
     * </table>
     *
     * @return virtual HTTP status, or {@code null} if no fault is present
     */
    public static Integer inferHttpStatus(String responseBody) {
        if (!hasFault(responseBody)) return null;
        // Client / Sender fault → treat as HTTP 400 (permanent, DLQ)
        if (isClientFault(responseBody)) return 400;
        // Server / Receiver fault or unknown → treat as HTTP 500 (transient, retry)
        return 500;
    }
}
