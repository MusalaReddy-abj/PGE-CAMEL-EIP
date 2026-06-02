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
import org.apache.camel.ProducerTemplate;
import org.springframework.beans.factory.annotation.Value;
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
 * Parses an FTP- or S3-delivered CSV file and publishes rows to Kafka in
 * configurable batches — bounding heap usage regardless of file size.
 *
 * <h3>Large-file strategy</h3>
 * <ol>
 *   <li><b>File-size guard</b> — reads the file-size header set by the FTP/S3
 *       consumer ({@code CamelFileSize} / {@code CamelAwsS3ContentLength}) and
 *       throws a {@link ValidationException} if it exceeds {@code kafkaBatchSize}
 *       MB. The file is moved to the error directory before any parsing begins.</li>
 *   <li><b>Streaming line-by-line parse</b> — the file is never fully materialised
 *       in heap; each line is read via a {@link BufferedReader} on the raw
 *       {@link InputStream}.</li>
 *   <li><b>Bounded batch publishing</b> — every {@code kafkaBatchSize} successfully
 *       parsed rows, the current batch is converted to {@link KafkaProfileReadPayload}
 *       objects and published to Kafka immediately via {@link ProducerTemplate}.
 *       The current batch list is then cleared. At any moment heap holds at most
 *       {@code kafkaBatchSize} rows.</li>
 *   <li><b>Per-row error isolation</b> — bad rows are collected into
 *       {@link LogConstants#PROP_FAILED_ROWS} for DLQ publishing without aborting
 *       the file.</li>
 * </ol>
 *
 * <h3>Exchange contract</h3>
 * <ul>
 *   <li><b>Body out</b> — {@code List<KafkaProfileReadPayload>} of the final
 *       (possibly partial) batch; published by the calling route.</li>
 *   <li><b>{@link LogConstants#PROP_FAILED_ROWS}</b> — always set (empty list when all rows OK).</li>
 *   <li><b>{@code profileReads.batchesPublished}</b> — total intermediate batches published.</li>
 * </ul>
 */
@Component
public class ProfileReadsCsvProcessor extends BaseProcessor {

    private static final StructuredLogger  log       = StructuredLogger.of(ProfileReadsCsvProcessor.class);
    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    // Camel header names for file-size check (FTP and S3 consumers respectively)
    private static final String HDR_FTP_SIZE = "CamelFileSize";
    private static final String HDR_S3_SIZE  = "CamelAwsS3ContentLength";

    private final ProfileReadsMapper profileReadsMapper;
    private final ProducerTemplate   producerTemplate;

    @Value("${kafka.topic.profile-reads:kraken-profile-reads-events}")
    private String profileReadsTopic;

    /** Rows per Kafka batch — injected from ftp/s3 profile-reads config or default 500. */
    @Value("${profile-reads.kafka-batch-size:500}")
    private int kafkaBatchSize;

    /** Max file size in MB — injected from ftp/s3 profile-reads config or default 100. */
    @Value("${profile-reads.max-file-size-mb:100}")
    private int maxFileSizeMb;

    public ProfileReadsCsvProcessor(ProfileReadsMapper profileReadsMapper,
                                     ProducerTemplate   producerTemplate) {
        this.profileReadsMapper = profileReadsMapper;
        this.producerTemplate   = producerTemplate;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String fileName      = exchange.getIn().getHeader(Exchange.FILE_NAME, String.class);

        // ── Step 1: file-size guard ───────────────────────────────────────────
        rejectIfOversized(exchange, correlationId);

        InputStream inputStream = exchange.getIn().getBody(InputStream.class);
        if (inputStream == null) {
            throw ValidationException.missingField("CSV body", correlationId);
        }

        List<ProfileReadPayload>   currentBatch = new ArrayList<>(kafkaBatchSize);
        List<ProfileReadFailedRow> failedRows   = new ArrayList<>();
        int dataRowsSeen       = 0;
        int intermediateBatches = 0;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

            // ── Step 2: header row ────────────────────────────────────────────
            String headerLine = reader.readLine();
            if (headerLine == null || headerLine.isBlank()) {
                throw ValidationException.missingField("CSV header", correlationId);
            }
            Map<String, Integer> headerIndex = profileReadsMapper.buildHeaderIndex(headerLine);
            profileReadsMapper.validateHeaders(headerIndex, correlationId);

            // ── Step 3: stream + batch-publish ────────────────────────────────
            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                String trimmed = line.trim();
                if (trimmed.isEmpty()) continue;

                dataRowsSeen++;
                try {
                    currentBatch.add(profileReadsMapper.parseRow(trimmed, headerIndex, correlationId));
                } catch (Exception e) {
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
                        "fileName", fileName, "lineNumber", lineNumber,
                        "errorType", e.getClass().getSimpleName(), "error", e.getMessage());
                }

                // Publish this batch and free memory
                if (currentBatch.size() >= kafkaBatchSize) {
                    publishBatch(currentBatch, correlationId);
                    intermediateBatches++;
                    log.debug("profileReadsBatchPublished", correlationId,
                        "batchNumber", intermediateBatches, "batchSize", currentBatch.size());
                    currentBatch.clear();
                }
            }
        }

        if (dataRowsSeen > 0 && currentBatch.isEmpty() && intermediateBatches == 0) {
            throw ValidationException.missingField(
                "parseable CSV rows (all " + dataRowsSeen + " rows failed)", correlationId);
        }

        if (!failedRows.isEmpty()) {
            log.warn("profileReadsCsvPartialFailure", correlationId,
                "fileName", fileName, "failed", failedRows.size());
        }

        // ── Step 4: convert remaining rows → final Kafka batch ────────────────
        List<KafkaProfileReadPayload> finalBatch =
            profileReadsMapper.toKafkaMessages(currentBatch, correlationId);

        log.info("profileReadsCsvProcessed", correlationId,
            "fileName",           fileName,
            "dataRows",           dataRowsSeen,
            "intermediateBatches", intermediateBatches,
            "finalBatch",         finalBatch.size(),
            "failedRows",         failedRows.size());

        // Final batch goes to the route's to("direct:publishToKafka")
        exchange.getIn().setBody(finalBatch);
        exchange.setProperty(LogConstants.PROP_FAILED_ROWS, failedRows);
        exchange.setProperty("profileReads.batchesPublished", intermediateBatches);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void rejectIfOversized(Exchange exchange, String correlationId) {
        long maxBytes = (long) maxFileSizeMb * 1024L * 1024L;

        Long ftpSize = exchange.getIn().getHeader(HDR_FTP_SIZE, Long.class);
        if (ftpSize != null && ftpSize > maxBytes) {
            throw ValidationException.invalidFormat("file size",
                "must be ≤ " + maxFileSizeMb + " MB (got " + (ftpSize / 1024 / 1024) + " MB)",
                correlationId);
        }

        Long s3Size = exchange.getIn().getHeader(HDR_S3_SIZE, Long.class);
        if (s3Size != null && s3Size > maxBytes) {
            throw ValidationException.invalidFormat("file size",
                "must be ≤ " + maxFileSizeMb + " MB (got " + (s3Size / 1024 / 1024) + " MB)",
                correlationId);
        }
    }

    private void publishBatch(List<ProfileReadPayload> batch, String correlationId) {
        List<KafkaProfileReadPayload> kafkaMessages =
            profileReadsMapper.toKafkaMessages(batch, correlationId);

        if (kafkaMessages.isEmpty()) return;

        producerTemplate.send("direct:publishToKafka", ex -> {
            ex.setProperty(LogConstants.KAFKA_TOPIC, profileReadsTopic);
            ex.setProperty(LogConstants.KAFKA_KEY,   correlationId);
            ex.getIn().setBody(kafkaMessages);
        });
    }
}
