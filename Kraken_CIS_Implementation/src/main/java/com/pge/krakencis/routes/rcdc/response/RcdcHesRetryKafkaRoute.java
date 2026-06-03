package com.pge.krakencis.routes.rcdc.response;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.rcdc.response.RcdcMdmNotificationProcessor;
import com.pge.krakencis.exceptions.RetryQueueExhaustedException;
import com.pge.krakencis.routes.BaseKafkaConsumerRoute;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.model.RouteDefinition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Retry-queue consumer for failed HES → MDM notifications.
 *
 * <p>Mirror of {@link RcdcRetryKafkaRoute} for the HES response flow.
 * Consumes from {@code kafka.topic.rcdc-hes-retry} and re-calls the MDM SOAP
 * service after a configurable delay (slow poll interval = back-off).
 *
 * <h3>Route ID</h3>
 * {@code route-rcdc-hes-retry-consumer}
 */
@Component
public class RcdcHesRetryKafkaRoute extends BaseKafkaConsumerRoute {

    private static final StructuredLogger log          = StructuredLogger.of(RcdcHesRetryKafkaRoute.class);
    private static final String           OPERATION    = "retryRcdcHesResponse";
    private static final String           SERVICE_NAME = "SOA-MDM-Service";

    private final RcdcMdmNotificationProcessor rcdcMdmNotificationProcessor;

    @Value("${kafka.topic.rcdc-hes-retry:kraken-rcdc-hes-retry-events}")
    private String hesRetryTopic;

    @Value("${kafka.topic.rcdc-hes-dlq:kraken-rcdc-hes-dlq-events}")
    private String hesDlqTopic;

    @Value("${retry.queue.max-attempts:3}")
    private int maxRetryQueueAttempts;

    @Value("${retry.queue.poll-delay-ms:300000}")
    private int retryPollDelayMs;

    public RcdcHesRetryKafkaRoute(CorrelationIdProcessor       correlationIdProcessor,
                                   RouteLoggingProcessor        routeLoggingProcessor,
                                   RouteExceptionProcessor      exceptionProcessor,
                                   RcdcMdmNotificationProcessor rcdcMdmNotificationProcessor) {
        super(correlationIdProcessor, routeLoggingProcessor, exceptionProcessor);
        this.rcdcMdmNotificationProcessor = rcdcMdmNotificationProcessor;
    }

    @Override
    public void configure() {
        final String uri =
            "kafka:{{kafka.topic.rcdc-hes-retry:kraken-rcdc-hes-retry-events}}"
            + "?brokers={{kafka.producer.brokers}}"
            + "&groupId={{kafka.consumer.group-id:kraken-cis-group}}-hes-retry"
            + "&autoOffsetReset={{kafka.consumer.auto-offset-reset:earliest}}"
            + "&maxPollRecords=50"
            + "&autoCommitEnable=false&allowManualCommit=true"
            + "&pollTimeoutMs=" + retryPollDelayMs
            + "&consumersCount={{kafka.consumer.consumers-count:1}}";

        RouteDefinition route = from(uri).routeId("route-rcdc-hes-retry-consumer");

        configureRetryQueueErrorHandlers(route, hesRetryTopic, hesDlqTopic, SERVICE_NAME);

        route
            .process(exchange -> exchange.setProperty(
                LogConstants.PROP_ORIGINAL_BODY, exchange.getIn().getBody(String.class)))
            .process(this::extractCorrelationIdFromRetry)
            .process(routeLoggingProcessor.entry(OPERATION))
            .process(exchange -> {
                int attempt = parseAttempt(exchange);
                if (attempt >= maxRetryQueueAttempts) {
                    String cid = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
                    log.error("rcdcHesRetryQueueExhausted", cid,
                        "attempt", attempt, "maxAttempts", maxRetryQueueAttempts);
                    setErrorProps(exchange, hesDlqTopic, "DLQ", SERVICE_NAME);
                    throw new RetryQueueExhaustedException(SERVICE_NAME, attempt);
                }
                log.info("rcdcHesRetryAttempt", exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                    "attempt", attempt + 1, "maxAttempts", maxRetryQueueAttempts);
            })
            .process(rcdcMdmNotificationProcessor)
            .process(this::commitOffset)
            .process(routeLoggingProcessor.exit(OPERATION));
    }

    private void extractCorrelationIdFromRetry(Exchange exchange) {
        String cid = exchange.getIn().getHeader("X-Correlation-ID", String.class);
        if (cid == null || cid.isBlank()) {
            cid = exchange.getIn().getHeader(KafkaConstants.KEY, String.class);
        }
        if (cid == null || cid.isBlank()) {
            cid = UUID.randomUUID().toString();
        }
        exchange.setProperty(LogConstants.PROP_CORRELATION_ID, cid);
        exchange.getIn().setHeader("X-Correlation-ID", cid);
        log.info("rcdcHesRetryMessageConsumed", cid,
            "retryTopic", exchange.getIn().getHeader(KafkaConstants.TOPIC),
            "errorType",  exchange.getIn().getHeader("X-Error-Type"),
            "prevStatus", exchange.getIn().getHeader("X-Error-Http-Status"));
    }

    private int parseAttempt(Exchange exchange) {
        String raw = exchange.getIn().getHeader("X-Error-Attempt", "0", String.class);
        try { return Integer.parseInt(raw); } catch (NumberFormatException e) { return 0; }
    }
}
