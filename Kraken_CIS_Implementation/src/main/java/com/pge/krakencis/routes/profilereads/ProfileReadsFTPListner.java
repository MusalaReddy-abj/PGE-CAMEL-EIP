package com.pge.krakencis.routes.profilereads;

import com.pge.krakencis.configs.ProfileReadsFtpProperties;
import com.pge.krakencis.exceptions.TransformationException;
import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.profilereads.ProfileReadsCsvProcessor;
import com.pge.krakencis.routes.BaseRoute;
import org.apache.camel.Exchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ProfileReadsFTPListner extends BaseRoute {

    private static final String OPERATION = "profileReadsFtpPoll";
    private static final StructuredLogger log = StructuredLogger.of(ProfileReadsFTPListner.class);

    private final ProfileReadsCsvProcessor profileReadsCsvProcessor;
    private final ProfileReadsFtpProperties   ftpProperties;

    @Value("${kafka.topic.profile-reads:kraken-profile-reads-events}")
    private String profileReadsTopic;

    public ProfileReadsFTPListner(ProfileReadsCsvProcessor profileReadsCsvProcessor,
                                    ProfileReadsFtpProperties ftpProperties) {
        this.profileReadsCsvProcessor = profileReadsCsvProcessor;
        this.ftpProperties            = ftpProperties;
    }

    @Override
    public void configure() {
        // Handle validation and transformation errors by moving file to error directory
        onException(ValidationException.class, TransformationException.class)
            .handled(false)
            .process(exchange -> moveFileToErrorDirectory(exchange))
            .end();

        processingRoute(ftpProperties.buildUri(), "route-profile-reads-ftp", OPERATION, route ->
            route
                .description("Poll ProfileReads CSV files from FTP, parse each row, and publish payloads to Kafka")
                .process(profileReadsCsvProcessor)
                .setProperty(LogConstants.KAFKA_TOPIC, constant(profileReadsTopic))
                .to("direct:publishToKafka")
        );
    }

    private void moveFileToErrorDirectory(Exchange exchange) {
        try {
            String fileName = exchange.getIn().getHeader(Exchange.FILE_NAME, String.class);
            String correlationId = (String) exchange.getProperty(LogConstants.PROP_CORRELATION_ID);
            String errorDir = ftpProperties.getErrorDirectory();
            
            if (fileName != null && !fileName.isEmpty()) {
                String errorPath = ftpProperties.buildErrorUri() + "/" + fileName;
                
                log.warn("movingFileToErrorDirectory", correlationId,
                    "fileName", fileName,
                    "errorDirectory", errorDir,
                    "targetPath", errorPath);
            }
        } catch (Exception e) {
            String correlationId = (String) exchange.getProperty(LogConstants.PROP_CORRELATION_ID);
            log.error("failedToMoveFileToErrorDirectory", correlationId,
                "exception", e.getMessage());
        }
    }
}
