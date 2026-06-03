package com.pge.krakencis.routes.profilereads;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.profilereads.ProfileReadFailedRow;
import com.pge.krakencis.models.profilereads.ProfileReadsAuditReport;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Publishes a file-processing audit report to the Kafka audit topic after every
 * Profile Reads CSV file completes (both FTP and S3 transports call this route).
 *
 * <h3>Route</h3>
 * {@code direct:publishProfileReadsAudit}
 *
 * <h3>Callers must have the following exchange properties set</h3>
 * (all populated automatically by {@link com.pge.krakencis.processors.profilereads.ProfileReadsCsvProcessor}):
 * <ul>
 *   <li>{@code Exchange.FILE_NAME}             — CSV file name</li>
 *   <li>{@link LogConstants#PROP_TOTAL_ROWS}   — total data rows seen</li>
 *   <li>{@link LogConstants#PROP_SUCCESS_ROWS} — rows successfully parsed</li>
 *   <li>{@link LogConstants#PROP_FAILED_ROWS}  — List of failed rows</li>
 *   <li>{@link LogConstants#PROP_CORRELATION_ID} — trace ID</li>
 * </ul>
 * Callers also pass the transport source ({@code FTP} or {@code S3}) as
 * exchange property {@code profileReads.source}.
 *
 * <h3>Kafka message published</h3>
 * <pre>
 * Topic key  = correlationId
 * Topic body = {
 *   "fileName":       "profilereads_20251218.csv",
 *   "totalRecords":   1000,
 *   "successRecords": 998,
 *   "failureRecords": 2,
 *   "correlationId":  "3fa85f64-...",
 *   "processedAt":    "2025-12-18T10:05:00+05:30",
 *   "source":         "FTP"
 * }
 * </pre>
 *
 * <h3>Route ID</h3>
 * {@code route-publish-profile-reads-audit}
 */
@Component
public class ProfileReadsAuditRoute extends RouteBuilder {

    private static final StructuredLogger log = StructuredLogger.of(ProfileReadsAuditRoute.class);

    @Value("${kafka.topic.profile-reads-audit:kraken-profile-reads-audit-events}")
    private String auditTopic;

    @Override
    public void configure() {

        from("direct:publishProfileReadsAudit")
            .routeId("route-publish-profile-reads-audit")
            // Build the audit report from exchange properties
            .process(this::buildAuditReport)
            // Set Kafka topic and key, then publish
            .setProperty(LogConstants.KAFKA_TOPIC, constant(auditTopic))
            .to("direct:publishToKafka")
            .process(exchange -> log.info("profileReadsAuditPublished",
                exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                "auditTopic",   auditTopic,
                "totalRecords", exchange.getProperty(LogConstants.PROP_TOTAL_ROWS)));
    }

    private void buildAuditReport(Exchange exchange) {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String fileName      = exchange.getIn().getHeader(Exchange.FILE_NAME, String.class);
        int    totalRows     = exchange.getProperty(LogConstants.PROP_TOTAL_ROWS,   0, Integer.class);
        int    successRows   = exchange.getProperty(LogConstants.PROP_SUCCESS_ROWS, 0, Integer.class);
        String source        = exchange.getProperty("profileReads.source",             "UNKNOWN", String.class);
        String explicitStatus = exchange.getProperty(LogConstants.PROP_FILE_STATUS,    String.class);
        String errorMessage  = exchange.getProperty(LogConstants.PROP_FILE_ERROR_MESSAGE, String.class);

        @SuppressWarnings("unchecked")
        List<ProfileReadFailedRow> failedRows =
            exchange.getProperty(LogConstants.PROP_FAILED_ROWS, List.class);
        int failureCount = (failedRows != null) ? failedRows.size() : Math.max(0, totalRows - successRows);

        // Resolve status: explicit value (set by error handler) takes priority,
        // otherwise infer from processing counts.
        String status = explicitStatus != null ? explicitStatus
            : (failureCount > 0 ? LogConstants.FILE_STATUS_PARTIAL_FAILURE
                                : LogConstants.FILE_STATUS_SUCCESS);

        ProfileReadsAuditReport report = ProfileReadsAuditReport.builder()
            .fileName(fileName)
            .totalRecords(totalRows)
            .successRecords(successRows)
            .failureRecords(failureCount)
            .correlationId(correlationId)
            .processedAt(OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
            .source(source)
            .status(status)
            .errorMessage(errorMessage)
            .build();

        exchange.setProperty(LogConstants.KAFKA_KEY, correlationId);
        exchange.getIn().setHeader(KafkaConstants.KEY, correlationId);
        exchange.getIn().setBody(report);
    }
}
