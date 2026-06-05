package com.pge.krakencis.routes.rcdc.request;

import com.pge.krakencis.exceptions.RetryQueueExhaustedException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.rcdc.request.RcdcTargetCallProcessor;
import com.pge.krakencis.processors.rcdc.request.RcdcTargetResponseProcessor;
import com.pge.krakencis.routes.BaseKafkaConsumerRoute;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.model.RouteDefinition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Retry-queue consumer for failed RCDC commands.
 *
 * <h3>When does a message arrive here?</h3>
 * After the main Kafka consumer ({@link RcdcRequestKafkaListner}) exhausts its
 * 3 in-process retries (1 s → 2 s → 4 s), the original message is published to
 * {@code kafka.topic.rcdc-retry} with error context as Kafka record headers.
 *
 * <h3>Retry strategy</h3>
 * <pre>
 * kraken-rcdc-retry-events
 *         │
 *  poll every 5 min (slow poll = back-off delay)
 *         │
 *  re-call SOA-RCDC using same processors
 *         │
 *  ┌──────┴───────┐
 *  │              │
 * OK           failed
 * offset      X-Error-Attempt &lt; maxRetryQueueAttempts?
 * committed    ┌───────┴──────┐
 *             yes             no
 *     republish to       final DLQ
 *     retry topic        (kraken-rcdc-dlq-events)
 *     (attempt+1)
 * </pre>
 *
 * <h3>Route ID</h3>
 * {@code route-rcdc-retry-consumer}
 */
@Component
public class RcdcRetryKafkaRoute extends BaseKafkaConsumerRoute {

    private static final StructuredLogger log          = StructuredLogger.of(RcdcRetryKafkaRoute.class);
    private static final String           OPERATION    = "retryRcdcRequest";
    private static final String           SERVICE_NAME = "SOA-RCDC-TargetService";

    private final RcdcTargetCallProcessor     rcdcTargetCallProcessor;
    private final RcdcTargetResponseProcessor rcdcTargetResponseProcessor;

    @Value("${kafka.topic.rcdc-retry:kraken-rcdc-retry-events}")
    private String rcdcRetryTopic;

    @Value("${kafka.topic.rcdc-dlq:kraken-rcdc-dlq-events}")
    private String rcdcDlqTopic;

    /** How many times a message may re-enter the retry queue before going to final DLQ. */
    @Value("${retry.queue.max-attempts:3}")
    private int maxRetryQueueAttempts;

    /** Poll interval for the retry topic (ms). Acts as the back-off delay. Default 5 min. */
    @Value("${retry.queue.poll-delay-ms:300000}")
    private int retryPollDelayMs;

    public RcdcRetryKafkaRoute(CorrelationIdProcessor      correlationIdProcessor,
                                RouteLoggingProcessor       routeLoggingProcessor,
                                RouteExceptionProcessor     exceptionProcessor,
                                RcdcTargetCallProcessor     rcdcTargetCallProcessor,
                                RcdcTargetResponseProcessor rcdcTargetResponseProcessor) {
        super(correlationIdProcessor, routeLoggingProcessor, exceptionProcessor);
        this.rcdcTargetCallProcessor     = rcdcTargetCallProcessor;
        this.rcdcTargetResponseProcessor = rcdcTargetResponseProcessor;
    }

    @Override
    public void configure() {
        // maxPollIntervalMs must exceed retryPollDelayMs (5 min) so Kafka does not
        // consider the consumer dead while the route delay() is sleeping.
        // Set to 2× the delay + 60s buffer = 660000 ms (11 min).
        long maxPollIntervalMs = retryPollDelayMs * 2L + 60_000L;

        final String uri =
            "kafka:{{kafka.topic.rcdc-retry:kraken-rcdc-retry-events}}"
            + "?brokers={{kafka.producer.brokers}}"
            + "&groupId={{kafka.consumer.group-id:kraken-cis-group}}-retry"
            + "&autoOffsetReset={{kafka.consumer.auto-offset-reset:earliest}}"
            + "&maxPollRecords=50"
            + "&autoCommitEnable=false&allowManualCommit=true"
            + "&maxPollIntervalMs=" + maxPollIntervalMs
            + "&consumersCount={{kafka.consumer.consumers-count:1}}"
            + securityQueryString();

        RouteDefinition route = from(uri).routeId("route-rcdc-retry-consumer");

        // Same error handlers as the main consumer (inherited from BaseKafkaConsumerRoute)
        // — on further failure: check attempt count → republish or final DLQ
        configureRetryQueueErrorHandlers(route, rcdcRetryTopic, rcdcDlqTopic, SERVICE_NAME);

        route
            .process(com.pge.krakencis.logging.SpanEnricher.kafkaConsume())
            .process(exchange -> exchange.setProperty(
                LogConstants.PROP_ORIGINAL_BODY, exchange.getIn().getBody(String.class)))
            .process(this::extractCorrelationIdFromRetry)
            .process(routeLoggingProcessor.entry(OPERATION))
            // 5-minute back-off before retrying the downstream call.
            // maxPollIntervalMs in the URI is set to 2× this value so Kafka does not
            // rebalance the consumer group while this thread is sleeping.
            .delay(retryPollDelayMs)

            // Check if this message has exceeded retry-queue attempt limit
            .process(exchange -> {
                int attempt = parseAttempt(exchange);
                if (attempt >= maxRetryQueueAttempts) {
                    String cid = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
                    log.error("rcdcRetryQueueExhausted", cid,
                        "attempt", attempt, "maxAttempts", maxRetryQueueAttempts);
                    setErrorProps(exchange, rcdcDlqTopic, "DLQ", SERVICE_NAME);
                    // Route directly to final DLQ and stop
                    throw new RetryQueueExhaustedException(SERVICE_NAME, attempt);
                }
                log.info("rcdcRetryAttempt", exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                    "attempt", attempt + 1, "maxAttempts", maxRetryQueueAttempts);
            })

            .process(rcdcTargetCallProcessor)
            .process(rcdcTargetResponseProcessor)
            .process(this::commitOffset)
            .process(routeLoggingProcessor.exit(OPERATION));
    }

    private void extractCorrelationIdFromRetry(Exchange exchange) {
        // Prefer X-Correlation-ID header set by KafkaErrorRoutes, fall back to Kafka key
        String cid = exchange.getIn().getHeader("X-Correlation-ID", String.class);
        if (cid == null || cid.isBlank()) {
            cid = exchange.getIn().getHeader(KafkaConstants.KEY, String.class);
        }
        if (cid == null || cid.isBlank()) {
            cid = UUID.randomUUID().toString();
        }
        exchange.setProperty(LogConstants.PROP_CORRELATION_ID, cid);
        exchange.getIn().setHeader("X-Correlation-ID", cid);
        log.info("rcdcRetryMessageConsumed", cid,
            "retryTopic", exchange.getIn().getHeader(KafkaConstants.TOPIC),
            "errorType",  exchange.getIn().getHeader("X-Error-Type"),
            "prevStatus", exchange.getIn().getHeader("X-Error-Http-Status"));
    }

    private int parseAttempt(Exchange exchange) {
        return parseKafkaHeaderAsInt(exchange, "X-Error-Attempt", 0);
    }

}
