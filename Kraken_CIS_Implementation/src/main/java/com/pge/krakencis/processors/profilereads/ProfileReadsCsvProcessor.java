package com.pge.krakencis.processors.profilereads;

import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.mappers.profilereads.ProfileReadsMapper;
import com.pge.krakencis.models.profilereads.KafkaProfileReadPayload;
import com.pge.krakencis.models.profilereads.ProfileReadPayload;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Parses an FTP-delivered CSV file and produces a list of
 * {@link KafkaProfileReadPayload} objects ready for Kafka publishing.
 *
 * <h3>Pipeline</h3>
 * <ol>
 *   <li><b>Parse</b> — streams the CSV line-by-line, maps each data row to a raw
 *       {@link ProfileReadPayload} via {@link ProfileReadsMapper#parseRow}.</li>
 *   <li><b>Group</b> — calls {@link ProfileReadsMapper#toKafkaMessages} to aggregate
 *       raw rows by mRID → ReadingType, producing one {@link KafkaProfileReadPayload}
 *       per unique mRID with nested registers and readings.</li>
 * </ol>
 *
 * <h3>Per-row error isolation</h3>
 * A bad data row is skipped and logged — it does not fail the whole file.
 * The file is moved to the error directory only when:
 * <ul>
 *   <li>The file body is empty.</li>
 *   <li>The header row is missing or lacks required columns.</li>
 *   <li>Every data row fails — no successful payloads at all.</li>
 * </ul>
 */
@Component
public class ProfileReadsCsvProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(ProfileReadsCsvProcessor.class);

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
            throw ValidationException.missingField("FTP CSV body", correlationId);
        }

        // ── Step 1: parse CSV rows ────────────────────────────────────────────
        List<ProfileReadPayload> rawRows = new ArrayList<>();
        int skippedRows  = 0;
        int dataRowsSeen = 0;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

            String headerLine = reader.readLine();
            if (headerLine == null || headerLine.isBlank()) {
                throw ValidationException.missingField("FTP CSV header", correlationId);
            }

            Map<String, Integer> headerIndex = profileReadsMapper.buildHeaderIndex(headerLine);
            profileReadsMapper.validateHeaders(headerIndex, correlationId);

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
                    skippedRows++;
                    log.warn("profileReadRowSkipped", correlationId,
                        "fileName",   fileName,
                        "lineNumber", lineNumber,
                        "errorType",  e.getClass().getSimpleName(),
                        "error",      e.getMessage());
                }
            }
        }

        if (dataRowsSeen > 0 && rawRows.isEmpty()) {
            throw ValidationException.missingField(
                "parseable CSV rows (all " + dataRowsSeen + " rows failed)", correlationId);
        }

        if (skippedRows > 0) {
            log.warn("profileReadsCsvPartialSuccess", correlationId,
                "fileName",  fileName,
                "parsed",    rawRows.size(),
                "skipped",   skippedRows);
        }

        // ── Step 2: group raw rows → Kafka messages ───────────────────────────
        // Groups by mRID, then ReadingType.
        // One KafkaProfileReadPayload per unique mRID → one Kafka message per mRID.
        List<KafkaProfileReadPayload> kafkaMessages =
            profileReadsMapper.toKafkaMessages(rawRows, correlationId);

        log.info("profileReadsCsvProcessed", correlationId,
            "fileName",      fileName,
            "csvRows",       rawRows.size(),
            "kafkaMessages", kafkaMessages.size(),
            "skipped",       skippedRows);

        exchange.getIn().setBody(kafkaMessages);
    }
}
