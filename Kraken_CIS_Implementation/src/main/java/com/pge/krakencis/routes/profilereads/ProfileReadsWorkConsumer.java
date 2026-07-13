package com.pge.krakencis.routes.profilereads;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pge.krakencis.configs.ProfileReadsS3Properties;
import com.pge.krakencis.exceptions.TransformationException;
import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.logging.RouteRootSpan;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.profilereads.ProfileReadsWorkItem;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.profilereads.ProfileReadsCsvProcessor;
import com.pge.krakencis.routes.BaseKafkaConsumerRoute;
import org.apache.camel.Exchange;
import org.apache.camel.model.RouteDefinition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Profile Reads — work-queue <b>consumer</b> (processing half of the work-queue pattern).
 *
 * <p>Consumes work-items ({@link ProfileReadsWorkItem}) from the work topic, fetches the
 * referenced S3 object, parses + publishes its rows (reusing {@link ProfileReadsCsvProcessor}),
 * and archives the file. Because both pods join one consumer group, each work-item — and
 * therefore each file — is handled by exactly one pod, while both pods stay busy across
 * different files. No leader election, no extra infra.
 *
 * <h3>Idempotency (the cross-pod duplicate fix)</h3>
 * The scheduler runs on every pod, so a file may yield duplicate work-items. They are keyed
 * by fileName (→ same partition → same consumer, processed sequentially), and this consumer
 * is idempotent: the first work-item archives the file; any later one finds it gone
 * ({@link NoSuchKeyException}) and <b>skips</b>. The archive move is the dedup marker.
 *
 * <p>Active only when an S3 bucket is configured AND
 * {@code profile-reads.ingestion.mode=work-queue} (default).
 */
@Component
@ConditionalOnExpression(
    "'${aws.s3.profile-reads.bucket-name:}' != '' and '${profile-reads.ingestion.mode:work-queue}' == 'work-queue'")
public class ProfileReadsWorkConsumer extends BaseKafkaConsumerRoute {

    private static final StructuredLogger log       = StructuredLogger.of(ProfileReadsWorkConsumer.class);
    private static final String           OPERATION = "profileReadsWorkConsume";
    private static final String           HDR_S3_SIZE = "CamelAwsS3ContentLength";
    private static final String           PROP_SKIP   = "profileReads.skip";
    private static final String           PROP_S3_KEY = "profileReads.s3Key";

    private final ProfileReadsCsvProcessor csvProcessor;
    private final ProfileReadsS3Properties s3Properties;
    private final S3Client                 s3Client;
    private final ObjectMapper             objectMapper;

    @Value("${kafka.topic.profile-reads:kraken-profile-reads-events}")
    private String profileReadsTopic;

    @Value("${profile-reads.work.max-poll-records:5}")
    private int maxPollRecords;

    @Value("${profile-reads.work.max-poll-interval-ms:1800000}")
    private int maxPollIntervalMs;

    public ProfileReadsWorkConsumer(CorrelationIdProcessor   correlationIdProcessor,
                                    RouteLoggingProcessor    routeLoggingProcessor,
                                    RouteExceptionProcessor  exceptionProcessor,
                                    ProfileReadsCsvProcessor csvProcessor,
                                    ProfileReadsS3Properties s3Properties,
                                    S3Client                 s3Client,
                                    ObjectMapper             objectMapper) {
        super(correlationIdProcessor, routeLoggingProcessor, exceptionProcessor);
        this.csvProcessor = csvProcessor;
        this.s3Properties = s3Properties;
        this.s3Client     = s3Client;
        this.objectMapper = objectMapper;
    }

    @Override
    public void configure() {
        final String uri =
            "kafka:{{kafka.topic.profile-reads-work:kraken-profile-reads-work-events}}"
            + "?brokers={{kafka.producer.brokers}}"
            + "&groupId={{kafka.consumer.group-id:kraken-cis-group}}-profile-reads-work"
            + "&autoOffsetReset={{kafka.consumer.auto-offset-reset:earliest}}"
            + "&maxPollRecords=" + maxPollRecords
            + "&autoCommitEnable=false&allowManualCommit=true"
            + "&consumersCount={{kafka.consumer.consumers-count:1}}"
            + "&maxPollIntervalMs=" + maxPollIntervalMs
            + "&heartbeatIntervalMs={{kafka.consumer.heartbeat-interval-ms:10000}}"
            + securityQueryString();

        RouteDefinition route = from(uri).routeId("route-profile-reads-work-consumer");

        // File-level error handling: move the file to the error prefix + audit, then commit.
        // (Not the Kafka retry/DLQ machinery — a bad CSV is a file problem, handled by moving it.)
        route.onException(Exception.class)
            .handled(true)
            .process(this::handleProcessingError)
            .process(this::commitOffset)
        .end();

        route
            .process(com.pge.krakencis.logging.KafkaTraceContext::adopt)
            .process(this::parseWorkItem)
            .process(exchange -> log.info("profileReadsWorkFetchStarted",
                exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                "key", exchange.getProperty(PROP_S3_KEY, String.class)))
            .process(this::fetchFromS3)
            .process(exchange -> log.info("profileReadsWorkFetchCompleted",
                exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                "key", exchange.getProperty(PROP_S3_KEY, String.class),
                "skip", exchange.getProperty(PROP_SKIP, false, Boolean.class)))
            .choice()
                // Idempotent skip: a prior work-item already processed + archived this file.
                .when(exchangeProperty(PROP_SKIP).isEqualTo(true))
                    .process(this::commitOffset)
                    .stop()
            .end()
            .process(routeLoggingProcessor.entry(OPERATION))
            .process(exchange -> log.info("profileReadsWorkCsvStarted",
                exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                "key", exchange.getProperty(PROP_S3_KEY, String.class)))
            .process(csvProcessor)
            .process(exchange -> log.info("profileReadsWorkCsvCompleted",
                exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                "totalRows", exchange.getProperty(LogConstants.PROP_TOTAL_ROWS),
                "successRows", exchange.getProperty(LogConstants.PROP_SUCCESS_ROWS)))
            .setProperty(LogConstants.KAFKA_TOPIC, constant(profileReadsTopic))
            .process(exchange -> log.info("profileReadsWorkPublishStarted",
                exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                "topic", profileReadsTopic))
            .to("direct:publishToKafka")
            .process(exchange -> log.info("profileReadsWorkPublishCompleted",
                exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                "topic", profileReadsTopic,
                "publishedCount", exchange.getIn().getBody()))
            .to("direct:publishProfileReadsDlq")
            .setProperty("profileReads.source", constant("S3"))
            .to("direct:publishProfileReadsAudit")
            .process(exchange -> log.info("profileReadsWorkArchiveStarted",
                exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                "key", exchange.getProperty(PROP_S3_KEY, String.class)))
            .process(this::moveToArchive)
            .process(exchange -> log.info("profileReadsWorkArchiveCompleted",
                exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                "key", exchange.getProperty(PROP_S3_KEY, String.class)))
            .process(routeLoggingProcessor.exit(OPERATION))
            .process(this::commitOffset);
    }

    // ── Steps ──────────────────────────────────────────────────────────────────

    /** Parses the work-item JSON and seeds correlation id + properties. */
    private void parseWorkItem(Exchange exchange) throws Exception {
        String json = exchange.getIn().getBody(String.class);
        ProfileReadsWorkItem item = objectMapper.readValue(json, ProfileReadsWorkItem.class);

        String correlationId = item.fileName();   // trace by file (also the Kafka key)
        exchange.setProperty(LogConstants.PROP_CORRELATION_ID, correlationId);
        exchange.setProperty(PROP_S3_KEY, item.key());
        exchange.getIn().setHeader("X-Correlation-ID", correlationId);
        exchange.getIn().setHeader(Exchange.FILE_NAME, item.fileName());

        // Stamp file identity on the (adopted) Kafka CONSUMER span so traces are searchable by
        // file in the backend (Jaeger tag `file.name`, Tempo `{ .file.name = "..." }`) — consistent
        // with the S3 poller route (ProfileReadsS3Listner). KafkaTraceContext.adopt() already made
        // the span current, so these land on it. `correlation_id` is stamped separately by
        // routeLoggingProcessor.entry() and equals fileName here.
        RouteRootSpan.attr(exchange, "file.name",  item.fileName());
        RouteRootSpan.attr(exchange, "aws.s3.key", item.key());

        log.info("profileReadsWorkReceived", correlationId, "bucket", item.bucket(), "key", item.key());
    }

    /**
     * Fetches the S3 object as the exchange body. If the object is gone
     * ({@link NoSuchKeyException}) a prior work-item already processed it → mark skip.
     */
    private void fetchFromS3(Exchange exchange) {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String bucket        = s3Properties.getBucketName();
        String key           = exchange.getProperty(PROP_S3_KEY, String.class);
        try {
            ResponseInputStream<GetObjectResponse> object = s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucket).key(key).build());
            exchange.getIn().setBody(object);   // InputStream — ProfileReadsCsvProcessor reads + closes it
            exchange.getIn().setHeader(HDR_S3_SIZE, object.response().contentLength());
        } catch (NoSuchKeyException e) {
            exchange.setProperty(PROP_SKIP, true);
            log.info("profileReadsWorkSkippedAlreadyProcessed", correlationId, "key", key);
        }
    }

    /** Success: copy the source object to the archive prefix, then delete the source. */
    private void moveToArchive(Exchange exchange) {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String bucket        = s3Properties.getBucketName();
        String sourceKey     = exchange.getProperty(PROP_S3_KEY, String.class);
        String fileName      = fileNameFrom(sourceKey);
        String archiveKey    = s3Properties.getArchivePrefix() + fileName;
        try {
            copyAndDelete(bucket, sourceKey, archiveKey);
            log.info("profileReadsWorkArchived", correlationId, "sourceKey", sourceKey, "archiveKey", archiveKey);
        } catch (Exception e) {
            // Rows were already published; archive is best-effort (e.g. a concurrent move won the race).
            log.warn("profileReadsWorkArchiveFailed", correlationId, "sourceKey", sourceKey, "error", e.getMessage());
        }
    }

    /** Error: move the file to the error prefix and publish a corrupted-file audit. */
    private void handleProcessingError(Exchange exchange) {
        Exception ex            = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        String    correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String    bucket        = s3Properties.getBucketName();
        String    sourceKey     = exchange.getProperty(PROP_S3_KEY, String.class);
        int       successRows   = exchange.getProperty(LogConstants.PROP_SUCCESS_ROWS, 0, Integer.class);

        boolean isFormatError = ex instanceof ValidationException || ex instanceof TransformationException;
        String status = (isFormatError || successRows == 0)
            ? LogConstants.FILE_STATUS_CORRUPTED
            : LogConstants.FILE_STATUS_PARTIAL_FAILURE;

        log.warn("profileReadsWorkFileFailed", correlationId,
            "key", sourceKey, "status", status,
            "errorType", ex != null ? ex.getClass().getSimpleName() : "unknown",
            "error", ex != null ? ex.getMessage() : "unknown",
            "successRowsBeforeFailure", successRows);

        if (sourceKey != null) {
            String errorKey = s3Properties.getErrorPrefix() + fileNameFrom(sourceKey);
            try {
                log.warn("profileReadsWorkErrorMoveStarted", correlationId,
                    "sourceKey", sourceKey, "errorKey", errorKey);
                copyAndDelete(bucket, sourceKey, errorKey);
                log.warn("profileReadsWorkErrorMoved", correlationId,
                    "sourceKey", sourceKey, "errorKey", errorKey);
            } catch (Exception e) {
                log.error("profileReadsWorkErrorMoveFailed", correlationId, e,
                    "sourceKey", sourceKey, "errorKey", errorKey);
            }
        } else {
            log.error("profileReadsWorkErrorMoveSkipped", correlationId,
                "reason", "missingSourceKey");
        }

        // Publish a file-level audit (same contract as the poller's corrupted-file audit).
        exchange.setProperty("profileReads.source", "S3");
        exchange.setProperty(LogConstants.PROP_FILE_STATUS, status);
        exchange.setProperty(LogConstants.PROP_FILE_ERROR_MESSAGE,
            ex != null ? ex.getClass().getSimpleName() + ": " + ex.getMessage() : "Unknown error");
        if (exchange.getIn().getHeader(Exchange.FILE_NAME) == null && sourceKey != null) {
            exchange.getIn().setHeader(Exchange.FILE_NAME, fileNameFrom(sourceKey));
        }
        try {
            exchange.getContext().createProducerTemplate()
                .send("direct:publishProfileReadsAudit", exchange);
            log.info("profileReadsWorkErrorAuditPublished", correlationId,
                "key", sourceKey, "status", status);
        } catch (Exception e) {
            log.error("profileReadsWorkErrorAuditFailed", correlationId, e,
                "key", sourceKey, "status", status);
        }
    }

    // ── S3 helpers ─────────────────────────────────────────────────────────────

    private void copyAndDelete(String bucket, String sourceKey, String destKey) {
        s3Client.copyObject(CopyObjectRequest.builder()
            .sourceBucket(bucket).sourceKey(sourceKey)
            .destinationBucket(bucket).destinationKey(destKey)
            .build());
        s3Client.deleteObject(DeleteObjectRequest.builder()
            .bucket(bucket).key(sourceKey)
            .build());
    }

    private static String fileNameFrom(String s3Key) {
        if (s3Key == null) return "unknown.csv";
        int lastSlash = s3Key.lastIndexOf('/');
        return lastSlash >= 0 ? s3Key.substring(lastSlash + 1) : s3Key;
    }
}
