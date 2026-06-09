package com.pge.krakencis.routes.profilereads;

import com.pge.krakencis.configs.ProfileReadsFtpProperties;
import com.pge.krakencis.exceptions.TransformationException;
import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.profilereads.ProfileReadsCsvProcessor;
import com.pge.krakencis.routes.BaseRoute;
import org.apache.camel.Exchange;
import org.apache.commons.net.ftp.FTPClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * FTP polling route that ingests Profile Reads CSV files and publishes each row
 * as an event to Kafka.
 *
 * <p>The FTP URI uses {@code noop=true} so the Camel FTP component never touches
 * the remote file after the exchange completes. All file movement is handled
 * explicitly by this route using a dedicated {@link FTPClient} connection, wrapped
 * in a try-catch that only logs on failure and never re-throws.
 *
 * <h3>Why noop=true instead of move=.done</h3>
 * <p>With the old {@code move=.done} approach, if the post-processing FTP rename
 * failed (e.g. transient FTP connectivity), Camel rolled back the idempotent
 * consumer entry — making the file eligible for reprocessing on the next poll and
 * producing duplicate Kafka messages. With {@code noop=true} the exchange always
 * completes successfully so the idempotent entry is never rolled back, regardless
 * of whether the archive rename succeeds.
 *
 * <h3>File lifecycle</h3>
 * <pre>
 *  FTP /incoming/file.csv
 *       │
 *       ├─ success → rename to {@code moveDirectory}/.done/file.csv   (best-effort)
 *       └─ error   → rename to {@code errorDirectory}/.error/file.csv (best-effort)
 * </pre>
 *
 * <h3>Route ID</h3>
 * {@code route-profile-reads-ftp}
 */
@Component
@ConditionalOnProperty(name = "ftp.listener.enabled", havingValue = "true")
public class ProfileReadsFTPListner extends BaseRoute {

    private static final String           OPERATION = "profileReadsFtpPoll";
    private static final StructuredLogger log       = StructuredLogger.of(ProfileReadsFTPListner.class);

    private final ProfileReadsCsvProcessor  profileReadsCsvProcessor;
    private final ProfileReadsFtpProperties ftpProperties;

    @Value("${kafka.topic.profile-reads:kraken-profile-reads-events}")
    private String profileReadsTopic;

    public ProfileReadsFTPListner(CorrelationIdProcessor   correlationIdProcessor,
                                   RouteLoggingProcessor    routeLoggingProcessor,
                                   RouteExceptionProcessor  exceptionProcessor,
                                   ProfileReadsCsvProcessor profileReadsCsvProcessor,
                                   ProfileReadsFtpProperties ftpProperties) {
        super(correlationIdProcessor, routeLoggingProcessor, exceptionProcessor);
        this.profileReadsCsvProcessor = profileReadsCsvProcessor;
        this.ftpProperties            = ftpProperties;
    }

    @Override
    public void configure() {
        // handled(true) — the exception is fully consumed here. The exchange completes
        // successfully from Camel's perspective so the idempotent consumer keeps the
        // file marked as "done" and never reprocesses it, even if the archive rename
        // below fails. The CORRUPTED audit and best-effort error-directory move are
        // both done here before cleanup.
        onException(Exception.class)
            .handled(true)
            .process(this::publishCorruptedFileAudit)
            .process(ex -> archiveFile(ex, ftpProperties.getErrorDirectory()))
            .process(exchange -> routeLoggingProcessor.cleanup(exchange))
            .end();

        from(ftpProperties.buildUri())
            .routeId("route-profile-reads-ftp")
            .description("Poll ProfileReads CSV files from FTP, parse each row, publish to Kafka")
            .threads(ftpProperties.getThreadPoolSize())
            // Start a root span for this poll — the Agent gives no entry span to S3/FTP/timer
            // consumers, so without this the FTP/Kafka-publish spans are disconnected orphans.
            .process(ex -> com.pge.krakencis.logging.RouteRootSpan.start(ex, "process profile-reads-ftp"))
            .process(correlationIdProcessor)
            .process(routeLoggingProcessor.entry(OPERATION))
            .process(profileReadsCsvProcessor)
            .setProperty(LogConstants.KAFKA_TOPIC, constant(profileReadsTopic))
            .to("direct:publishToKafka")
            .to("direct:publishProfileReadsDlq")
            .setProperty("profileReads.source", constant("FTP"))
            .to("direct:publishProfileReadsAudit")
            // Best-effort rename to archive directory. Never throws — a rename failure
            // only produces a warning log; it does not fail the exchange or trigger
            // idempotent rollback.
            .process(ex -> archiveFile(ex, ftpProperties.getMoveDirectory()))
            .process(routeLoggingProcessor.exit(OPERATION));
    }

    private void publishCorruptedFileAudit(Exchange exchange) {
        Exception ex            = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        String    fileName      = exchange.getIn().getHeader(Exchange.FILE_NAME, String.class);
        String    correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        int       successRows   = exchange.getProperty(LogConstants.PROP_SUCCESS_ROWS, 0, Integer.class);

        // Format errors (ValidationException / TransformationException) always mean CORRUPTED —
        // the file itself is malformed regardless of how many rows were published before detection.
        // Other exceptions mid-stream where some rows succeeded → PARTIAL_FAILURE.
        boolean isFormatError = ex instanceof ValidationException || ex instanceof TransformationException;
        String status = isFormatError || successRows == 0
            ? LogConstants.FILE_STATUS_CORRUPTED
            : LogConstants.FILE_STATUS_PARTIAL_FAILURE;

        log.warn("profileReadFileFailed", correlationId,
            "fileName",  fileName,
            "status",    status,
            "errorType", ex != null ? ex.getClass().getSimpleName() : "unknown",
            "error",     ex != null ? ex.getMessage() : "unknown",
            "successRowsBeforeFailure", successRows);

        exchange.setProperty("profileReads.source",               "FTP");
        exchange.setProperty(LogConstants.PROP_FILE_STATUS,       status);
        exchange.setProperty(LogConstants.PROP_FILE_ERROR_MESSAGE,
            ex != null ? ex.getClass().getSimpleName() + ": " + ex.getMessage() : "Unknown error");

        exchange.getContext().createProducerTemplate()
            .send("direct:publishProfileReadsAudit", exchange);
    }

    /**
     * Renames the remote file from {@code remoteDirectory/fileName} to
     * {@code destDirectory/fileName} using a short-lived {@link FTPClient} connection.
     *
     * <p>Never throws — any failure is logged as a warning and silently swallowed so
     * the Camel exchange always completes successfully. This preserves the idempotent
     * consumer's "done" entry and prevents the file being reprocessed on the next poll.
     */
    private void archiveFile(Exchange exchange, String destDirectory) {
        String fileName      = exchange.getIn().getHeader(Exchange.FILE_NAME, String.class);
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);

        if (fileName == null || destDirectory == null) {
            log.warn("ftpArchiveSkipped", correlationId,
                "reason", "fileName or destDirectory is null");
            return;
        }

        String sourcePath = ftpProperties.getRemoteDirectory() + "/" + fileName;
        String destPath   = destDirectory + "/" + fileName;

        FTPClient client = new FTPClient();
        try {
            client.connect(ftpProperties.getConnection().getHost(),
                           ftpProperties.getConnection().getPort());
            client.login(ftpProperties.getConnection().getUsername(),
                         ftpProperties.getConnection().getPassword());
            if (ftpProperties.isPassiveMode()) {
                client.enterLocalPassiveMode();
            }

            boolean renamed = client.rename(sourcePath, destPath);
            if (renamed) {
                log.info("ftpFileArchived", correlationId,
                    "fileName", fileName, "destDirectory", destDirectory);
            } else {
                log.warn("ftpArchiveRenameFailed", correlationId,
                    "fileName",  fileName,
                    "source",    sourcePath,
                    "dest",      destPath,
                    "ftpReply",  client.getReplyString().trim());
            }
        } catch (Exception e) {
            log.warn("ftpArchiveException", correlationId,
                "fileName", fileName, "destDirectory", destDirectory, "error", e.getMessage());
            // Intentionally not re-thrown — archive failure must never fail the exchange.
        } finally {
            try {
                if (client.isConnected()) client.disconnect();
            } catch (Exception ignored) {}
        }
    }
}
