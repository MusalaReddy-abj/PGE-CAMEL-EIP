package com.pge.krakencis.models.profilereads;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

/**
 * Kafka DLQ event published when an individual CSV row fails to parse or validate.
 *
 * <p>Published to {@code kafka.topic.profile-reads-dlq} so failed rows can be
 * inspected and replayed without losing the surrounding context.
 *
 * <h3>Example</h3>
 * <pre>
 * {
 *   "fileName":     "profilereads_20251218.csv",
 *   "lineNumber":   7,
 *   "rawLine":      "CREATED,MeterReadings,,0.0.0.4...,2025-12-18T00:30:00+05:30,0.0",
 *   "errorType":    "TransformationException",
 *   "errorMessage": "Required field is missing: mRID",
 *   "correlationId":"3fa85f64-5717-4562-b3fc-2c963f66afa6",
 *   "timestamp":    "2025-12-18T10:00:00+05:30"
 * }
 * </pre>
 */
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProfileReadFailedRow {

    /** Name of the source CSV file. */
    private String fileName;

    /** 1-based line number within the file (header = line 1). */
    private int lineNumber;

    /** Original CSV line exactly as it appeared in the file. */
    private String rawLine;

    /** Java exception simple class name (e.g. {@code TransformationException}). */
    private String errorType;

    /** Human-readable error message. */
    private String errorMessage;

    /** Correlation ID from the file-processing exchange. */
    private String correlationId;

    /** ISO-8601 timestamp when the row failed. */
    private String timestamp;
}
