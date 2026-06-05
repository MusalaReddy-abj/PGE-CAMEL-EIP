package com.pge.krakencis.routes.profilereads;

import com.pge.krakencis.configs.ProfileReadsS3Properties;
import com.pge.krakencis.exceptions.TransformationException;
import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.profilereads.ProfileReadsCsvProcessor;
import com.pge.krakencis.routes.BaseRoute;
import jakarta.annotation.PostConstruct;
import org.apache.camel.Exchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * S3 polling route that ingests Profile Reads CSV files from an AWS S3 bucket
 * and publishes each row as an event to Kafka.
 *
 * <p>Mirrors {@link ProfileReadsFTPListner} exactly — same processing pipeline,
 * same Kafka topic, same error behaviour — only the transport differs (S3 vs FTP).
 *
 * <h3>Reused components</h3>
 * <ul>
 *   <li>{@link ProfileReadsCsvProcessor} — CSV parsing and validation (shared with FTP route)</li>
 *   <li>{@code direct:publishToKafka} — Kafka publishing (shared with all routes)</li>
 * </ul>
 *
 * <h3>File lifecycle</h3>
 * <pre>
 *  S3 bucket / incoming/file.csv
 *       │
 *       ├─ success → copy to archive/{file.csv} → delete original
 *       └─ error   → copy to error/{file.csv}   → delete original
 * </pre>
 *
 * <h3>Activation</h3>
 * This route is only created when {@code aws.s3.profile-reads.bucket-name} is
 * configured. If the property is absent, the bean is not registered and no S3
 * connection is attempted.
 *
 * <h3>Route ID</h3>
 * {@code route-profile-reads-s3}
 */
@Component
@ConditionalOnProperty(prefix = "aws.s3.profile-reads", name = "bucket-name")
public class ProfileReadsS3Listner extends BaseRoute {

    private static final String           OPERATION = "profileReadsS3Poll";
    private static final StructuredLogger log       = StructuredLogger.of(ProfileReadsS3Listner.class);

    // ── Camel header names set by the aws2-s3 consumer ───────────────────────
    private static final String HDR_BUCKET = "CamelAwsS3BucketName";
    private static final String HDR_KEY    = "CamelAwsS3Key";

    private final ProfileReadsCsvProcessor  profileReadsCsvProcessor;
    private final ProfileReadsS3Properties  s3Properties;
    private final S3Client                  s3Client;

    @Value("${kafka.topic.profile-reads:kraken-profile-reads-events}")
    private String profileReadsTopic;

    public ProfileReadsS3Listner(CorrelationIdProcessor   correlationIdProcessor,
                                  RouteLoggingProcessor    routeLoggingProcessor,
                                  RouteExceptionProcessor  exceptionProcessor,
                                  ProfileReadsCsvProcessor profileReadsCsvProcessor,
                                  ProfileReadsS3Properties s3Properties,
                                  S3Client                 s3Client) {
        super(correlationIdProcessor, routeLoggingProcessor, exceptionProcessor);
        this.profileReadsCsvProcessor = profileReadsCsvProcessor;
        this.s3Properties             = s3Properties;
        this.s3Client                 = s3Client;
    }

    /** Creates the source-prefix placeholder once at startup so the folder is visible immediately. */
    @PostConstruct
    public void createSourceFolderPlaceholder() {
        recreateSourceFolder(s3Properties.getBucketName());
    }

    @Override
    public void configure() {
        onException(Exception.class)
            .handled(true)
            .process(this::moveToArchiveOnError)
            .process(this::publishCorruptedFileAudit)
            .process(exchange -> routeLoggingProcessor.cleanup(exchange))
            .end();

        from(s3Properties.buildUri())
            .routeId("route-profile-reads-s3")
            .description("Poll ProfileReads CSV files from S3, parse each row, publish to Kafka")
            // No .threads() — process each polled object synchronously on the consumer
            // thread. This prevents a later poll cycle from picking up the same object
            // while a large file is still being processed (which caused the same file to
            // be published to Kafka twice and a NoSuchKey 404 when both tried to archive).
            .process(correlationIdProcessor)
            .choice()
                // Only .csv files (case-insensitive) are processed.
                .when(header(HDR_KEY).regex("(?i).*\\.csv"))
                    .process(routeLoggingProcessor.entry(OPERATION))
                    .process(profileReadsCsvProcessor)
                    .setProperty(LogConstants.KAFKA_TOPIC, constant(profileReadsTopic))
                    .to("direct:publishToKafka")
                    .to("direct:publishProfileReadsDlq")
                    .setProperty("profileReads.source", constant("S3"))
                    .to("direct:publishProfileReadsAudit")
                    .process(this::moveToArchive)
                    .process(routeLoggingProcessor.exit(OPERATION))
                // .keep placeholder: silently complete — stays in place, folder remains visible.
                .when(header(HDR_KEY).endsWith(".keep"))
                    .stop()
                // S3 "folder" placeholder objects — a zero-byte object whose key ends in "/"
                // (e.g. the prefix marker "Reads/profilereads/" itself). These are NOT files;
                // leave them in place. Moving/deleting them is what made the folder disappear
                // and produced spurious "unsupported file" → Error moves every poll.
                .when(header(HDR_KEY).endsWith("/"))
                    .stop()
                // Any other file type (not .csv): skip entirely — do NOT parse, do NOT
                // publish an audit record, do NOT move. The object is left untouched in
                // the source prefix and simply ignored on every poll.
                .otherwise()
                    .process(exchange -> log.debug("s3NonCsvSkipped",
                        exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                        "s3Key", exchange.getIn().getHeader(HDR_KEY, String.class),
                        "reason", "only .csv files are processed"))
                    .stop()
            .end();
    }

    // ── S3 file lifecycle helpers ─────────────────────────────────────────────

    private void moveToArchive(Exchange exchange) {
        String bucket     = exchange.getIn().getHeader(HDR_BUCKET, String.class);
        String sourceKey  = exchange.getIn().getHeader(HDR_KEY,    String.class);
        String cid        = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String fileName   = fileNameFrom(sourceKey);
        String archiveKey = s3Properties.getArchivePrefix() + fileName;

        try {
            copyAndDelete(bucket, sourceKey, archiveKey);
            log.info("s3FileArchived", cid,
                "bucket", bucket, "sourceKey", sourceKey, "archiveKey", archiveKey);
        } catch (Exception e) {
            // File may have been deleted externally or by the S3 consumer internally.
            // Log and continue — Kafka messages were already published successfully.
            log.warn("s3ArchiveFailed", cid, "bucket", bucket,
                "sourceKey", sourceKey, "error", e.getMessage());
        }
    }

    /**
     * Called from the error handler: moves the corrupted/failed file to the
     * error prefix ({@code Reads/Error/}) so it is clearly separated from
     * successfully processed files in the archive prefix.
     */
    private void moveToArchiveOnError(Exchange exchange) {
        String bucket    = exchange.getIn().getHeader(HDR_BUCKET, String.class);
        String sourceKey = exchange.getIn().getHeader(HDR_KEY,    String.class);
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);

        if (bucket == null || sourceKey == null) {
            log.warn("s3CorruptedFileMoveSkipped", correlationId,
                "reason", "bucket or key header missing");
            return;
        }

        String fileName  = fileNameFrom(sourceKey);
        String errorKey  = s3Properties.getErrorPrefix() + fileName;

        try {
            copyAndDelete(bucket, sourceKey, errorKey);
            log.info("s3CorruptedFileMovedToError", correlationId,
                "bucket", bucket, "sourceKey", sourceKey, "errorKey", errorKey);
        } catch (Exception e) {
            log.error("s3CorruptedFileMoveToErrorFailed", correlationId, e,
                "bucket", bucket, "sourceKey", sourceKey);
        }
    }

    private void publishCorruptedFileAudit(Exchange exchange) {
        Exception ex            = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        String    sourceKey     = exchange.getIn().getHeader(HDR_KEY, String.class);
        String    correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        int       successRows   = exchange.getProperty(LogConstants.PROP_SUCCESS_ROWS, 0, Integer.class);

        // Format errors (ValidationException / TransformationException) always mean CORRUPTED —
        // the file itself is malformed regardless of how many rows were published before detection.
        // Other exceptions mid-stream where some rows succeeded → PARTIAL_FAILURE.
        boolean isFormatError = ex instanceof ValidationException || ex instanceof TransformationException;
        String status = isFormatError || successRows == 0
            ? LogConstants.FILE_STATUS_CORRUPTED
            : LogConstants.FILE_STATUS_PARTIAL_FAILURE;

        log.warn("s3ProfileReadFileFailed", correlationId,
            "s3Key",     sourceKey,
            "status",    status,
            "errorType", ex != null ? ex.getClass().getSimpleName() : "unknown",
            "error",     ex != null ? ex.getMessage() : "unknown",
            "successRowsBeforeFailure", successRows);

        // FILE_NAME header might not be set for S3; fall back to the S3 key name
        if (exchange.getIn().getHeader(Exchange.FILE_NAME) == null && sourceKey != null) {
            exchange.getIn().setHeader(Exchange.FILE_NAME, fileNameFrom(sourceKey));
        }

        exchange.setProperty("profileReads.source",               "S3");
        exchange.setProperty(LogConstants.PROP_FILE_STATUS,       status);
        exchange.setProperty(LogConstants.PROP_FILE_ERROR_MESSAGE,
            ex != null ? ex.getClass().getSimpleName() + ": " + ex.getMessage() : "Unknown error");

        exchange.getContext().createProducerTemplate()
            .send("direct:publishProfileReadsAudit", exchange);
    }

    // Copy to destination, then delete the source. deleteAfterRead=false in the
    // S3 URI means Camel never touches the object, so we own the full lifecycle.
    // Synchronous polling (no .threads()) guarantees the source still exists here.
    private void copyAndDelete(String bucket, String sourceKey, String destKey) {
        s3Client.copyObject(CopyObjectRequest.builder()
            .sourceBucket(bucket).sourceKey(sourceKey)
            .destinationBucket(bucket).destinationKey(destKey)
            .build());

        s3Client.deleteObject(DeleteObjectRequest.builder()
            .bucket(bucket).key(sourceKey)
            .build());
    }

    private void recreateSourceFolder(String bucket) {
        String placeholderKey = s3Properties.getSourcePrefix() + ".keep";
        try {
            s3Client.putObject(
                PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(placeholderKey)
                    .build(),
                RequestBody.empty());
        } catch (Exception e) {
            log.warn("s3SourceFolderRecreateFailed", null,
                "bucket", bucket, "key", placeholderKey, "error", e.getMessage());
        }
    }

    private static String fileNameFrom(String s3Key) {
        if (s3Key == null) return "unknown.csv";
        int lastSlash = s3Key.lastIndexOf('/');
        return lastSlash >= 0 ? s3Key.substring(lastSlash + 1) : s3Key;
    }
}
