package com.pge.krakencis.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Standard error response body returned by all HTTP listener routes.
 *
 * <h3>Example — validation failure (400)</h3>
 * <pre>
 * {
 *   "status":        400,
 *   "errorCode":     "VAL-002",
 *   "message":       "Required field is missing: payload.events",
 *   "correlationId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
 *   "timestamp":     "2025-12-18T10:00:00+05:30",
 *   "detail":        ["payload.events must not be empty"]
 * }
 * </pre>
 *
 * <h3>Example — service unavailable after retries (503)</h3>
 * <pre>
 * {
 *   "status":        503,
 *   "errorCode":     "SYS-001",
 *   "message":       "Service temporarily unavailable after 3 retry attempt(s)",
 *   "correlationId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
 *   "timestamp":     "2025-12-18T10:00:05+05:30",
 *   "detail": {
 *     "retriesAttempted": 3,
 *     "cause":            "Connection refused: kafka:9092"
 *   }
 * }
 * </pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ErrorResponse {

    /** HTTP status code mirrored in the body for client convenience. */
    private int status;

    /** Domain error code — e.g. VAL-001, TRF-001, SYS-001. */
    private String errorCode;

    /** Human-readable error description. */
    private String message;

    /** Correlation ID of the failed request for end-to-end tracing. */
    private String correlationId;

    /** ISO-8601 timestamp of the error. */
    private String timestamp;

    /**
     * Optional structured detail.
     * <ul>
     *   <li>Validation (400): {@code List<String>} of violation messages.</li>
     *   <li>Service unavailable (503): {@code Map} with {@code retriesAttempted} and {@code cause}.</li>
     *   <li>Other errors: {@code null} — field omitted from JSON.</li>
     * </ul>
     */
    private Object detail;
}
