package com.pge.krakencis.models.profilereads;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

/**
 * Audit report published to {@code kafka.topic.profile-reads-audit} after every
 * CSV file is processed — whether fully successful or partially failed.
 *
 * <h3>Kafka message key</h3>
 * The correlation ID of the file-processing exchange so all related events
 * (CSV rows on {@code kraken-profile-reads-events} and failed rows on
 * {@code kraken-profile-reads-dlq-events}) share the same key.
 *
 * <h3>Example</h3>
 * <pre>
 * {
 *   "fileName":      "profilereads_20251218.csv",
 *   "totalRecords":  1000,
 *   "successRecords": 998,
 *   "failureRecords":   2,
 *   "correlationId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
 *   "processedAt":   "2025-12-18T10:05:00+05:30",
 *   "source":        "FTP"
 * }
 * </pre>
 */
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProfileReadsAuditReport {

    /** Original file name as received from FTP / S3. */
    private String fileName;

    /** Total data rows seen in the file (excludes the header row). */
    private int totalRecords;

    /** Rows successfully parsed and published to Kafka. */
    private int successRecords;

    /** Rows that failed parsing or validation — published to the DLQ topic. */
    private int failureRecords;

    /** End-to-end trace ID for correlating all events from this file. */
    private String correlationId;

    /** ISO-8601 timestamp when processing completed. */
    private String processedAt;

    /** Transport source: {@code FTP} or {@code S3}. */
    private String source;
}
