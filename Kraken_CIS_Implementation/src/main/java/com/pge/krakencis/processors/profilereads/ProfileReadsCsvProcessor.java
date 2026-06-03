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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Parses an FTP- or S3-delivered CSV file and publishes rows to Kafka in
 * configurable batches — bounding heap usage regardless of file size.
 *
 * <h3>Multi-file performance</h3>
 * <p>The FTP/S3 routes call this processor from a thread pool
 * ({@code .threads(threadPoolSize)}). Each file is processed by a separate
 * thread, so {@code threadPoolSize} files are handled in parallel.
 *
 * <h3>Per-file performance — async batch publishing</h3>
 * <p>Intermediate Kafka batches are submitted via
 * {@link ProducerTemplate#asyncSend} so the CSV parser does not block waiting
 * for Kafka acknowledgements. Up to {@code maxConcurrentBatches} batches are
 * allowed in-flight simultaneously, providing back-pressure. All futures are
 * awaited before the exchange completes so errors surface correctly.
 *
 * <h3>Grouping performance</h3>
 * <p>{@code toKafkaMessages()} uses parallel streams for files with more than
 * {@link #PARALLEL_THRESHOLD} rows, taking advantage of multiple CPU cores.
 *
 * <h3>Exchange contract</h3>
 * <ul>
 *   <li>Body out → {@code List<KafkaProfileReadPayload>} final batch</li>
 *   <li>{@link LogConstants#PROP_FAILED_ROWS} → failed rows for DLQ</li>
 *   <li>{@link LogConstants#PROP_TOTAL_ROWS} / {@link LogConstants#PROP_SUCCESS_ROWS} → audit</li>
 * </ul>
 */
@Component
public class ProfileReadsCsvProcessor extends BaseProcessor {

    private static final StructuredLogger  log               = StructuredLogger.of(ProfileReadsCsvProcessor.class);
    private static final DateTimeFormatter TIMESTAMP         = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    private static final String            HDR_FTP_SIZE      = "CamelFileSize";
    private static final String            HDR_S3_SIZE       = "CamelAwsS3ContentLength";

    /** Minimum row count before parallel stream grouping is used. */
    private static final int PARALLEL_THRESHOLD = 2_000;

    /** Timeout waiting for each async Kafka batch future. */
    private static final long FUTURE_TIMEOUT_S = 30;

    private final ProfileReadsMapper profileReadsMapper;
    private final ProducerTemplate   producerTemplate;

    @Value("${kafka.topic.profile-reads:kraken-profile-reads-events}")
    private String profileReadsTopic;

    @Value("${profile-reads.kafka-batch-size:500}")
    private int kafkaBatchSize;

    @Value("${profile-reads.max-file-size-mb:100}")
    private int maxFileSizeMb;

    /**
     * Max Kafka batches in-flight simultaneously per file.
     * Prevents unbounded memory usage when Kafka is slower than the CSV parser.
     */
    @Value("${profile-reads.max-concurrent-batches:4}")
    private int maxConcurrentBatches;

    public ProfileReadsCsvProcessor(ProfileReadsMapper profileReadsMapper,
                                     ProducerTemplate   producerTemplate) {
        this.profileReadsMapper = profileReadsMapper;
        this.producerTemplate   = producerTemplate;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String fileName      = exchange.getIn().getHeader(Exchange.FILE_NAME, String.class);

        rejectIfOversized(exchange, correlationId);

        InputStream inputStream = exchange.getIn().getBody(InputStream.class);
        if (inputStream == null) {
            throw ValidationException.missingField("CSV body", correlationId);
        }

        List<ProfileReadPayload>   currentBatch  = new ArrayList<>(kafkaBatchSize);
        List<ProfileReadFailedRow> failedRows    = new ArrayList<>();
        List<Future<Exchange>>     batchFutures  = new ArrayList<>();
        int dataRowsSeen        = 0;
        int intermediateBatches = 0;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

            // ── Header ────────────────────────────────────────────────────────
            String headerLine = reader.readLine();
            if (headerLine == null || headerLine.isBlank()) {
                throw ValidationException.missingField("CSV header", correlationId);
            }
            Map<String, Integer> headerIndex = profileReadsMapper.buildHeaderIndex(headerLine);
            profileReadsMapper.validateHeaders(headerIndex, correlationId);

            // ── Stream rows + async batch publish ─────────────────────────────
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

                // When batch is full, fire-and-forget async Kafka publish
                if (currentBatch.size() >= kafkaBatchSize) {
                    // Back-pressure: wait for oldest batch if too many are in-flight
                    if (batchFutures.size() >= maxConcurrentBatches) {
                        waitForOldestBatch(batchFutures, correlationId);
                    }
                    batchFutures.add(publishBatchAsync(currentBatch, correlationId));
                    intermediateBatches++;
                    log.debug("profileReadsBatchFired", correlationId,
                        "batchNumber", intermediateBatches,
                        "batchSize",   currentBatch.size(),
                        "inFlight",    batchFutures.size());
                    currentBatch = new ArrayList<>(kafkaBatchSize); // fresh list (old one owned by async task)
                }
            }
        }

        if (dataRowsSeen > 0 && currentBatch.isEmpty() && intermediateBatches == 0) {
            throw ValidationException.missingField(
                "parseable CSV rows (all " + dataRowsSeen + " rows failed)", correlationId);
        }

        // ── Wait for all in-flight async batches to complete ──────────────────
        awaitAllBatches(batchFutures, correlationId);

        // ── Group remaining rows and set as exchange body for the route ───────
        // Use parallel stream for large files (≥ PARALLEL_THRESHOLD rows)
        List<KafkaProfileReadPayload> finalBatch = currentBatch.size() >= PARALLEL_THRESHOLD
            ? profileReadsMapper.toKafkaMessagesParallel(currentBatch, correlationId)
            : profileReadsMapper.toKafkaMessages(currentBatch, correlationId);

        int successRows = dataRowsSeen - failedRows.size();

        if (!failedRows.isEmpty()) {
            log.warn("profileReadsCsvPartialFailure", correlationId,
                "fileName", fileName, "failed", failedRows.size());
        }

        log.info("profileReadsCsvProcessed", correlationId,
            "fileName",            fileName,
            "dataRows",            dataRowsSeen,
            "intermediateBatches", intermediateBatches,
            "finalBatch",          finalBatch.size(),
            "failedRows",          failedRows.size());

        exchange.getIn().setBody(finalBatch);
        exchange.setProperty(LogConstants.PROP_FAILED_ROWS,   failedRows);
        exchange.setProperty(LogConstants.PROP_TOTAL_ROWS,    dataRowsSeen);
        exchange.setProperty(LogConstants.PROP_SUCCESS_ROWS,  successRows);
        exchange.setProperty("profileReads.batchesPublished", intermediateBatches);
    }

    // ── Async Kafka publishing ────────────────────────────────────────────────

    /**
     * Fires a Kafka batch asynchronously and returns a {@link Future} to track
     * completion. Does NOT block the CSV parsing thread.
     */
    private Future<Exchange> publishBatchAsync(List<ProfileReadPayload> batch,
                                                String correlationId) {
        List<KafkaProfileReadPayload> kafkaMessages =
            profileReadsMapper.toKafkaMessages(batch, correlationId);

        if (kafkaMessages.isEmpty()) return completedFuture();

        return producerTemplate.asyncSend("direct:publishToKafka", ex -> {
            ex.setProperty(LogConstants.KAFKA_TOPIC, profileReadsTopic);
            ex.setProperty(LogConstants.KAFKA_KEY,   correlationId);
            ex.getIn().setBody(kafkaMessages);
        });
    }

    /** Waits for the oldest in-flight batch (index 0) to free a slot. */
    private void waitForOldestBatch(List<Future<Exchange>> futures,
                                     String correlationId) throws Exception {
        if (futures.isEmpty()) return;
        Future<Exchange> oldest = futures.remove(0);
        oldest.get(FUTURE_TIMEOUT_S, TimeUnit.SECONDS);
        log.debug("profileReadsBatchCompleted", correlationId,
            "remainingInFlight", futures.size());
    }

    /** Waits for ALL remaining in-flight batch futures to complete. */
    private void awaitAllBatches(List<Future<Exchange>> futures,
                                  String correlationId) throws Exception {
        for (Future<Exchange> f : futures) {
            f.get(FUTURE_TIMEOUT_S, TimeUnit.SECONDS);
        }
        if (!futures.isEmpty()) {
            log.debug("profileReadsAllBatchesCompleted", correlationId,
                "batchCount", futures.size());
        }
    }

    /** Returns an already-completed future for empty batches. */
    private Future<Exchange> completedFuture() {
        return java.util.concurrent.CompletableFuture.completedFuture(null);
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
}
