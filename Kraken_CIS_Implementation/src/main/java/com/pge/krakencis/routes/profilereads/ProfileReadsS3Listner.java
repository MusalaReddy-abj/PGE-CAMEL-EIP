package com.pge.krakencis.routes.profilereads;

import com.pge.krakencis.configs.ProfileReadsS3Properties;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.profilereads.ProfileReadsCsvProcessor;
import com.pge.krakencis.routes.BaseRoute;
import org.apache.camel.Exchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

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

    @Override
    public void configure() {
        // handled=false — exception propagates so Camel marks the exchange failed.
        // We still move the file to the error prefix before propagating.
        onException(Exception.class)
            .handled(false)
            .process(this::moveToError)
            .process(this::logFileError)
            .end();

        from(s3Properties.buildUri())
            .routeId("route-profile-reads-s3")
            .description("Poll ProfileReads CSV files from S3, parse each row, publish to Kafka")
            .threads(s3Properties.getThreadPoolSize())
            .process(correlationIdProcessor)
            .process(routeLoggingProcessor.entry(OPERATION))
            .process(profileReadsCsvProcessor)                                  // ← reused from FTP flow
            .setProperty(LogConstants.KAFKA_TOPIC, constant(profileReadsTopic))
            .to("direct:publishToKafka")                                        // ← reused from FTP flow
            .to("direct:publishProfileReadsDlq")                                // partial-failure rows → DLQ
            .setProperty("profileReads.source", constant("S3"))
            .to("direct:publishProfileReadsAudit")                              // file-level audit report → audit topic
            .process(this::moveToArchive)
            .process(routeLoggingProcessor.exit(OPERATION));
    }

    // ── S3 file lifecycle helpers ─────────────────────────────────────────────

    private void moveToArchive(Exchange exchange) {
        String bucket   = exchange.getIn().getHeader(HDR_BUCKET, String.class);
        String sourceKey = exchange.getIn().getHeader(HDR_KEY,    String.class);
        String fileName  = fileNameFrom(sourceKey);
        String archiveKey = s3Properties.getArchivePrefix() + fileName;

        copyAndDelete(bucket, sourceKey, archiveKey);

        log.info("s3FileArchived",
            exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
            "bucket", bucket, "sourceKey", sourceKey, "archiveKey", archiveKey);
    }

    private void moveToError(Exchange exchange) {
        String bucket    = exchange.getIn().getHeader(HDR_BUCKET, String.class);
        String sourceKey = exchange.getIn().getHeader(HDR_KEY,    String.class);
        if (bucket == null || sourceKey == null) return;

        String fileName = fileNameFrom(sourceKey);
        String errorKey = s3Properties.getErrorPrefix() + fileName;

        try {
            copyAndDelete(bucket, sourceKey, errorKey);
            log.warn("s3FileMovedToError",
                exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                "bucket", bucket, "sourceKey", sourceKey, "errorKey", errorKey);
        } catch (Exception e) {
            log.error("s3FileMoveToErrorFailed",
                exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class), e,
                "bucket", bucket, "sourceKey", sourceKey);
        }
    }

    private void copyAndDelete(String bucket, String sourceKey, String destKey) {
        s3Client.copyObject(CopyObjectRequest.builder()
            .sourceBucket(bucket).sourceKey(sourceKey)
            .destinationBucket(bucket).destinationKey(destKey)
            .build());

        s3Client.deleteObject(DeleteObjectRequest.builder()
            .bucket(bucket).key(sourceKey)
            .build());
    }

    private void logFileError(Exchange exchange) {
        Exception ex          = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        String    key         = exchange.getIn().getHeader(HDR_KEY, String.class);
        String    correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);

        log.warn("s3ProfileReadFileProcessingFailed", correlationId,
            "s3Key",     key,
            "errorType", ex != null ? ex.getClass().getSimpleName() : "unknown",
            "error",     ex != null ? ex.getMessage() : "unknown");
    }

    private static String fileNameFrom(String s3Key) {
        if (s3Key == null) return "unknown.csv";
        int lastSlash = s3Key.lastIndexOf('/');
        return lastSlash >= 0 ? s3Key.substring(lastSlash + 1) : s3Key;
    }
}
