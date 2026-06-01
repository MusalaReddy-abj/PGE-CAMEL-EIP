package com.pge.krakencis.exceptions;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract root for all domain exceptions in the Kraken CIS integration layer.
 *
 * <p>Every exception carries:
 * <ul>
 *   <li>{@link ErrorCode} — a typed, prefixed code (e.g. {@code EXT-003}) for
 *       programmatic handling and alerting.</li>
 *   <li>{@link ErrorCategory} — broad grouping used by exception handlers to select
 *       an HTTP status code ({@code VALIDATION} → 400, etc.).</li>
 *   <li>{@code correlationId} — the end-to-end trace ID of the exchange that raised
 *       the exception, enabling log correlation.</li>
 *   <li>{@code context} map — arbitrary key/value pairs added via
 *       {@link #withContext(String, Object)} to enrich the error with diagnostic
 *       data (e.g. the field name that failed validation, the HTTP status returned
 *       by a downstream service).</li>
 * </ul>
 *
 * <p>{@code withContext} returns {@code KrakenBaseException} (the base type).
 * Subclass factory methods should store the result in a local variable before
 * calling {@code withContext} to avoid an unsafe downcast.
 */
public abstract class KrakenBaseException extends RuntimeException {

    private final ErrorCode errorCode;
    private final ErrorCategory errorCategory;
    private final String correlationId;
    private final Instant timestamp;
    private final Map<String, Object> context;

    protected KrakenBaseException(ErrorCode errorCode, ErrorCategory errorCategory,
                                   String message, String correlationId) {
        super(message);
        this.errorCode       = errorCode;
        this.errorCategory   = errorCategory;
        this.correlationId   = correlationId;
        this.timestamp       = Instant.now();
        this.context         = new HashMap<>();
    }

    protected KrakenBaseException(ErrorCode errorCode, ErrorCategory errorCategory,
                                   String message, String correlationId, Throwable cause) {
        super(message, cause);
        this.errorCode       = errorCode;
        this.errorCategory   = errorCategory;
        this.correlationId   = correlationId;
        this.timestamp       = Instant.now();
        this.context         = new HashMap<>();
    }

    public KrakenBaseException withContext(String key, Object value) {
        this.context.put(key, value);
        return this;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public ErrorCategory getErrorCategory() {
        return errorCategory;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Map<String, Object> getContext() {
        return Collections.unmodifiableMap(context);
    }

    @Override
    public String toString() {
        return String.format("[%s][%s] correlationId=%s message=%s",
            errorCategory, errorCode.getCode(), correlationId, getMessage());
    }
}
