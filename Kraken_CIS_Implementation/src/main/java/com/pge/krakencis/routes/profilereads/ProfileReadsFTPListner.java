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
        // handled=false lets the exception propagate to the Camel FTP component so
        // it can apply moveFailed and move the file to the error directory.
        onException(Exception.class)
            .handled(false)
            .process(this::logFileError)
            .end();

        from(ftpProperties.buildUri())
            .routeId("route-profile-reads-ftp")
            .description("Poll ProfileReads CSV files from FTP, parse each row, publish to Kafka")
            // threadPoolSize allows multiple downloaded files to be processed concurrently.
            // The FTP consumer still polls one file at a time; .threads() decouples
            // downstream processing so the next poll can start while prior files are in-flight.
            .threads(ftpProperties.getThreadPoolSize())
            .process(correlationIdProcessor)
            .process(routeLoggingProcessor.entry(OPERATION))
            .process(profileReadsCsvProcessor)
            .setProperty(LogConstants.KAFKA_TOPIC, constant(profileReadsTopic))
            .to("direct:publishToKafka")
            .process(routeLoggingProcessor.exit(OPERATION));
    }

    private void logFileError(Exchange exchange) {
        Exception ex          = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        String    fileName    = exchange.getIn().getHeader(Exchange.FILE_NAME, String.class);
        String    correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);

        log.warn("profileReadFileProcessingFailed", correlationId,
            "fileName",  fileName,
            "errorType", ex != null ? ex.getClass().getSimpleName() : "unknown",
            "error",     ex != null ? ex.getMessage() : "unknown");
    }
}
