package com.pge.krakencis.processors.profilereads;

import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.mappers.profilereads.ProfileReadsMapper;
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
 * Parses an FTP-delivered CSV file into a list of {@link ProfileReadPayload} objects
 * ready for Kafka publishing.
 *
 * <h3>Streaming</h3>
 * <p>The file body is read as an {@link InputStream} and processed line-by-line via
 * a {@link BufferedReader}. The full file content is never materialised as a single
 * {@code String}, so even very large CSV files do not cause heap pressure.
 *
 * <h3>Per-row error isolation</h3>
 * <p>If an individual data row fails parsing or validation, that row is skipped and
 * logged as a warning — it does not fail the entire file. The exchange body is set to
 * the list of successfully parsed payloads.
 *
 * <p>The file is moved to {@code .error} (and the exchange fails) only when:
 * <ul>
 *   <li>The file body is empty or missing.</li>
 *   <li>The header line is absent or missing required columns.</li>
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

        List<ProfileReadPayload> payloads = new ArrayList<>();
        int skippedRows  = 0;
        int dataRowsSeen = 0;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

            // ── Header ────────────────────────────────────────────────────────
            String headerLine = reader.readLine();
            if (headerLine == null || headerLine.isBlank()) {
                throw ValidationException.missingField("FTP CSV header", correlationId);
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
                    payloads.add(profileReadsMapper.parseRow(trimmed, headerIndex, correlationId));
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

        // ── Outcome ───────────────────────────────────────────────────────────
        if (dataRowsSeen > 0 && payloads.isEmpty()) {
            throw ValidationException.missingField(
                "parseable CSV rows (all " + dataRowsSeen + " rows failed)", correlationId);
        }

        if (skippedRows > 0) {
            log.warn("profileReadsCsvPartialSuccess", correlationId,
                "fileName",    fileName,
                "published",   payloads.size(),
                "skipped",     skippedRows);
        }

        log.info("profileReadsCsvParsed", correlationId,
            "fileName",     fileName,
            "payloadCount", payloads.size(),
            "skippedCount", skippedRows);

        exchange.getIn().setBody(payloads);
    }
}
