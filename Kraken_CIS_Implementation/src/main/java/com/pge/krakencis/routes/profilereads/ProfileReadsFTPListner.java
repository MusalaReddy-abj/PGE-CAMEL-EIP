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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * FTP polling route that ingests Profile Reads CSV files and publishes each row
 * as an event to Kafka.
 *
 * <p>The FTP URI is assembled by {@link ProfileReadsFtpProperties#buildUri()}.
 * Successfully processed files are moved to the {@code moveDirectory} (default:
 * {@code .done}). Files that fail validation or transformation are moved to the
 * {@code errorDirectory} (default: {@code .error}) via the Camel FTP
 * {@code moveFailed} option — no manual file movement is needed in Java code.
 *
 * <h3>Error handling strategy</h3>
 * <ul>
 *   <li>{@link ValidationException} / {@link TransformationException} — the error
 *       is logged with {@code handled=false} so the exception propagates to the
 *       Camel FTP component, which moves the file to the {@code moveFailed}
 *       directory.</li>
 *   <li>All other exceptions — caught by the same {@code handled=false} handler so
 *       the file is also moved to the error directory.</li>
 * </ul>
 *
 * <h3>Route ID</h3>
 * {@code route-profile-reads-ftp}
 */
@Component
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
        // handled=true — exception is consumed by our handler so the Camel FTP component
        // sees the exchange as successfully completed. FTP therefore applies the `move`
        // option (Archive directory) instead of `moveFailed` (Error directory).
        // The handler publishes a CORRUPTED audit record before cleaning up.
        onException(Exception.class)
            .handled(true)
            .process(this::publishCorruptedFileAudit)
            .process(exchange -> routeLoggingProcessor.cleanup(exchange))
            .end();

        from(ftpProperties.buildUri())
            .routeId("route-profile-reads-ftp")
            .description("Poll ProfileReads CSV files from FTP, parse each row, publish to Kafka")
            .threads(ftpProperties.getThreadPoolSize())
            .process(correlationIdProcessor)
            .process(routeLoggingProcessor.entry(OPERATION))
            .process(profileReadsCsvProcessor)
            .setProperty(LogConstants.KAFKA_TOPIC, constant(profileReadsTopic))
            .to("direct:publishToKafka")
            .to("direct:publishProfileReadsDlq")
            .setProperty("profileReads.source", constant("FTP"))
            .to("direct:publishProfileReadsAudit")
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
}
