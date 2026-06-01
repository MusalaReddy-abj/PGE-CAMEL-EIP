package com.pge.krakencis.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

/**
 * Kafka message payload published to the retry queue or DLQ when an outbound
 * HTTP call fails after all in-process retry attempts are exhausted.
 *
 * <p>The {@link #eventType} field distinguishes the two destination topics:
 * <ul>
 *   <li>{@code RETRY} — transient failure; a retry-queue consumer should
 *       re-attempt the call after a configurable delay.</li>
 *   <li>{@code DLQ} — permanent failure (non-2xx client error, or retry-queue
 *       consumer also exhausted); requires manual investigation.</li>
 * </ul>
 */
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OutboundRequestEvent {

    /** {@code RETRY} or {@code DLQ}. */
    private String eventType;

    private String correlationId;

    /** Logical name of the downstream service (e.g. {@code SOA-RCDC-TargetService}). */
    private String serviceName;

    private String url;
    private String method;
    private String contentType;

    /** Original request body. May be null for GETs or if body logging is suppressed. */
    private String body;

    /** Attempt number on which the final failure occurred. */
    private int attempt;

    /** Maximum attempts configured for this call. */
    private int maxAttempts;

    /**
     * HTTP status code returned by the server.
     * {@code 0} indicates a connection-level failure (no HTTP response received).
     */
    private int httpStatus;

    private String errorType;
    private String errorMessage;

    /** ISO-8601 timestamp of the failure. */
    private String timestamp;
}
