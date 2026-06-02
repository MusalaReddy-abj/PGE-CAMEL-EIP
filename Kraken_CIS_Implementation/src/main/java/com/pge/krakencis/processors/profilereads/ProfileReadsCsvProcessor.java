package com.pge.krakencis.processors.profilereads;

import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.mappers.profilereads.ProfileReadsMapper;
import com.pge.krakencis.models.profilereads.KafkaProfileReadPayload;
import com.pge.krakencis.models.profilereads.ProfileReadFailedRow;
import com.pge.krakencis.models.profilereads.ProfileReadPayload;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Parses an FTP- or S3-delivered CSV file and produces:
 *
 * <ul>
 *   <li><b>Exchange body</b> — {@code List<KafkaProfileReadPayload>} of successfully
 *       parsed rows, grouped by mRID + ReadingType, ready for Kafka publishing.</li>
 *   <li><b>Exchange property {@link LogConstants#PROP_FAILED_ROWS}</b> —
 *       {@code List<ProfileReadFailedRow>} of rows that failed to parse or validate.
 *       Always set (empty list when all rows succeed). The calling route publishes
 *       these to the profile-reads DLQ topic via
 *       {@code direct:publishProfileReadsDlq}.</li>
 * </ul>
 *
 * <h3>Outcome summary</h3>
 * <table border="1">
 *   <tr><th>Condition</th><th>Body</th><th>PROP_FAILED_ROWS</th><th>Exception thrown</th></tr>
 *   <tr><td>All rows OK</td><td>All payloads</td><td>empty list</td><td>—</td></tr>
 *   <tr><td>Some rows fail</td><td>Good payloads</td><td>failed rows → DLQ</td><td>—</td></tr>
 *   <tr><td>All rows fail</td><td>—</td><td>—</td><td>ValidationException → error dir</td></tr>
 *   <tr><td>Header missing/corrupt</td><td>—</td><td>—</td><td>ValidationException → error dir</td></tr>
 * </table>
 */
@Component
public class ProfileReadsCsvProcessor extends BaseProcessor {

    private static final StructuredLogger   log       = StructuredLogger.of(ProfileReadsCsvProcessor.class);
    private static final DateTimeFormatter  TIMESTAMP = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private final ProfileReadsMapper profileReadsMapper;

    public ProfileReadsCsvProcessor(ProfileReadsMapper profileReadsMapper) {
        this.profileReadsMapper = profileReadsMapper;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String fileName      = exchange.getIn().getHeader(Exchange.FILE_NAME, String.class);

        InputStream inputStream = exchange.getIn().getBody(InputStream.class);
        if (inputStream == null) {
            throw ValidationException.missingField("CSV body", correlationId);
        }

        List<ProfileReadPayload>  rawRows    = new ArrayList<>();
        List<ProfileReadFailedRow> failedRows = new ArrayList<>();
        int dataRowsSeen = 0;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

            // ── Header ────────────────────────────────────────────────────────
            String headerLine = reader.readLine();
            if (headerLine == null || headerLine.isBlank()) {
                throw ValidationException.missingField("CSV header", correlationId);
            }

            Map<String, Integer> headerIndex = profileReadsMapper.buildHeaderIndex(headerLine);
            profileReadsMapper.validateHeaders(headerIndex, correlationId);

            // ── Data rows — per-row isolation ─────────────────────────────────
            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                String trimmed = line.trim();
                if (trimmed.isEmpty()) continue;

                dataRowsSeen++;
                try {
                    rawRows.add(profileReadsMapper.parseRow(trimmed, headerIndex, correlationId));
                } catch (Exception e) {
                    // Row failed — collect for DLQ, do not fail the whole file
                    failedRows.add(ProfileReadFailedRow.builder()
                        .fileName(fileName)
                        .lineNumber(lineNumber)
                        .rawLine(trimmed)
                        .errorType(e.getClass().getSimpleName())
                        .errorMessage(e.getMessage())
                        .correlationId(correlationId)
                        .timestamp(OffsetDateTime.now().format(TIMESTAMP))
                        .build());

                    log.warn("profileReadRowSkipped", correlationId,
                        "fileName",   fileName,
                        "lineNumber", lineNumber,
                        "errorType",  e.getClass().getSimpleName(),
                        "error",      e.getMessage());
                }
            }
        }

        // ── Fail the whole file only when every row is bad ────────────────────
        if (dataRowsSeen > 0 && rawRows.isEmpty()) {
            throw ValidationException.missingField(
                "parseable CSV rows (all " + dataRowsSeen + " rows failed)", correlationId);
        }

        if (!failedRows.isEmpty()) {
            log.warn("profileReadsCsvPartialFailure", correlationId,
                "fileName",    fileName,
                "successful",  rawRows.size(),
                "failed",      failedRows.size());
        }

        // ── Group successful rows → Kafka messages ────────────────────────────
        List<KafkaProfileReadPayload> kafkaMessages =
            profileReadsMapper.toKafkaMessages(rawRows, correlationId);

        log.info("profileReadsCsvProcessed", correlationId,
            "fileName",      fileName,
            "csvRows",       rawRows.size(),
            "kafkaMessages", kafkaMessages.size(),
            "failedRows",    failedRows.size());

        // Exchange body  = successful payloads → published to main topic
        // Exchange prop  = failed rows → published to DLQ by the calling route
        exchange.getIn().setBody(kafkaMessages);
        exchange.setProperty(LogConstants.PROP_FAILED_ROWS, failedRows);
    }
}
