package com.pge.krakencis.routes.profilereads;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.profilereads.ProfileReadFailedRow;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Shared Camel route that publishes partial-failure rows from CSV processing
 * to the Profile Reads DLQ Kafka topic.
 *
 * <h3>Contract for callers</h3>
 * <p>Before calling {@code to("direct:publishProfileReadsDlq")}, the exchange must
 * have {@link LogConstants#PROP_FAILED_ROWS} set to a
 * {@code List<ProfileReadFailedRow>} (set automatically by
 * {@link com.pge.krakencis.processors.profilereads.ProfileReadsCsvProcessor}).
 *
 * <p>The route preserves the exchange body so callers can continue their
 * normal flow (e.g. the response processor still sees the published count).
 *
 * <h3>Route ID</h3>
 * {@code route-publish-profile-reads-dlq}
 */
@Component
public class ProfileReadsDlqRoute extends RouteBuilder {

    private static final StructuredLogger log = StructuredLogger.of(ProfileReadsDlqRoute.class);

    static final String METRIC_DLQ_ROWS = "kafka.dlq.rows.published";

    @Value("${kafka.topic.profile-reads-dlq:kraken-profile-reads-dlq-events}")
    private String profileReadsDlqTopic;

    private final MeterRegistry meterRegistry;

    public ProfileReadsDlqRoute(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void configure() {

        from("direct:publishProfileReadsDlq")
            .routeId("route-publish-profile-reads-dlq")
            .process(exchange -> {
                @SuppressWarnings("unchecked")
                List<ProfileReadFailedRow> failedRows =
                    exchange.getProperty(LogConstants.PROP_FAILED_ROWS, List.class);

                if (failedRows == null || failedRows.isEmpty()) {
                    return; // nothing to publish — exit early
                }

                String correlationId = exchange.getProperty(
                    LogConstants.PROP_CORRELATION_ID, String.class);

                // Save the current body (success count from publishToKafka) so we can
                // restore it after publishing the failed rows.
                Object savedBody = exchange.getIn().getBody();

                // Swap body to failed rows and publish to DLQ topic
                exchange.getIn().setBody(failedRows);
                exchange.setProperty(LogConstants.KAFKA_TOPIC, profileReadsDlqTopic);
                exchange.setProperty(LogConstants.KAFKA_KEY, correlationId);

                log.warn("profileReadsDlqPublishing", correlationId,
                    "dlqTopic",   profileReadsDlqTopic,
                    "failedRows", failedRows.size());

                // Restore original body after DLQ processing completes
                exchange.setProperty("_profileReads.savedBody", savedBody);
            })
            .choice()
                // Only route to Kafka if there are actually failed rows
                .when(exchangeProperty(LogConstants.PROP_FAILED_ROWS)
                    .method("size").isGreaterThan(0))
                    .setProperty(LogConstants.KAFKA_TOPIC, constant(profileReadsDlqTopic))
                    .to("direct:publishToKafka")
                    .process(exchange -> {
                        @SuppressWarnings("unchecked")
                        List<ProfileReadFailedRow> rows =
                            exchange.getProperty(LogConstants.PROP_FAILED_ROWS, List.class);
                        int rowCount = rows != null ? rows.size() : 0;
                        meterRegistry.counter(METRIC_DLQ_ROWS,
                            "topic", profileReadsDlqTopic).increment(rowCount);
                        log.warn("profileReadsDlqPublished",
                            exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                            "dlqTopic",   profileReadsDlqTopic,
                            "rowsQueued", rowCount);
                    })
            .end()
            // Restore the body that was in place before this route was called
            .process(exchange -> {
                Object savedBody = exchange.getProperty("_profileReads.savedBody");
                if (savedBody != null) {
                    exchange.getIn().setBody(savedBody);
                }
            });
    }
}
