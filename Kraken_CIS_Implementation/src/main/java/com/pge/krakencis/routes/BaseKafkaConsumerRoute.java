package com.pge.krakencis.routes;

import com.pge.krakencis.exceptions.ExternalServiceException;
import com.pge.krakencis.exceptions.RetryableException;
import com.pge.krakencis.exceptions.TransformationException;
import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.model.RouteDefinition;

import java.util.UUID;

/**
 * Base class for all Kafka consumer routes.
 *
 * <p>Eliminates duplication across {@code RcdcRequestKafkaListner} and
 * {@code RcdcResponseHesKafkaListner} by providing:
 *
 * <ul>
 *   <li>{@link #configureKafkaErrorHandlers} — wires the four {@code onException}
 *       blocks (payload error → DLQ, 4xx → DLQ, retryable → retry queue,
 *       unknown → retry queue) onto a given route definition.</li>
 *   <li>{@link #extractCorrelationId} — reads the Kafka message key as the
 *       correlation ID, generating a UUID if absent.</li>
 *   <li>{@link #commitOffset} — manually commits the Kafka consumer offset.</li>
 *   <li>{@link #setErrorProps} — prepares exchange properties before routing
 *       to {@code direct:publishToRetryQueue} or {@code direct:publishToDlq}.</li>
 * </ul>
 */
public abstract class BaseKafkaConsumerRoute extends BaseRoute {

    private static final StructuredLogger log         = StructuredLogger.of(BaseKafkaConsumerRoute.class);
    protected static final int            MAX_RETRIES = 3;

    protected BaseKafkaConsumerRoute(CorrelationIdProcessor  correlationIdProcessor,
                                     RouteLoggingProcessor   routeLoggingProcessor,
                                     RouteExceptionProcessor exceptionProcessor) {
        super(correlationIdProcessor, routeLoggingProcessor, exceptionProcessor);
    }

    // ── Shared error handler configuration ───────────────────────────────────

    /**
     * Wires all four Kafka consumer error handlers onto {@code route}:
     *
     * <ol>
     *   <li>Payload errors ({@link ValidationException}, {@link TransformationException})
     *       → DLQ immediately.</li>
     *   <li>Client errors ({@link ExternalServiceException}) → DLQ immediately.</li>
     *   <li>{@link RetryableException} (5xx / 404 / 408 / 429 / network)
     *       → retry 3× (1 s → 2 s → 4 s) → retry queue.</li>
     *   <li>{@link Exception} catch-all (possibly transient)
     *       → retry 3× → retry queue.</li>
     * </ol>
     *
     * @param route       the route definition to attach handlers to
     * @param retryTopic  Kafka topic for transient failures after retries
     * @param dlqTopic    Kafka topic for permanent failures
     * @param serviceName downstream service name written to error headers
     */
    protected void configureKafkaErrorHandlers(RouteDefinition route,
                                                String retryTopic,
                                                String dlqTopic,
                                                String serviceName) {
        // Payload errors → DLQ immediately (no retry)
        route.onException(ValidationException.class, TransformationException.class)
            .handled(true)
            .process(exchange -> setErrorProps(exchange, dlqTopic, "DLQ", serviceName))
            .to("direct:publishToDlq")
            .process(this::commitOffset)
        .end();

        // 4xx client error → DLQ immediately (permanent)
        route.onException(ExternalServiceException.class)
            .handled(true)
            .process(exchange -> setErrorProps(exchange, dlqTopic, "DLQ", serviceName))
            .to("direct:publishToDlq")
            .process(this::commitOffset)
        .end();

        // Transient errors → retry 3× → retry queue
        route.onException(RetryableException.class)
            .maximumRedeliveries(MAX_RETRIES)
            .redeliveryDelay(1_000)
            .backOffMultiplier(2)
            .useExponentialBackOff()
            .retryAttemptedLogLevel(LoggingLevel.WARN)
            .handled(true)
            .process(exchange -> setErrorProps(exchange, retryTopic, "RETRY", serviceName))
            .to("direct:publishToRetryQueue")
            .process(this::commitOffset)
        .end();

        // Unknown errors — may be transient → retry 3× → retry queue
        route.onException(Exception.class)
            .maximumRedeliveries(MAX_RETRIES)
            .redeliveryDelay(1_000)
            .backOffMultiplier(2)
            .useExponentialBackOff()
            .retryAttemptedLogLevel(LoggingLevel.WARN)
            .handled(true)
            .process(exchange -> setErrorProps(exchange, retryTopic, "RETRY", serviceName))
            .to("direct:publishToRetryQueue")
            .process(this::commitOffset)
        .end();
    }

    // ── Shared helper methods ─────────────────────────────────────────────────

    /**
     * Reads the Kafka message key as the correlation ID; generates a UUID if absent.
     *
     * @param logEvent structured log event name (e.g. {@code "rcdcMessageConsumed"})
     */
    protected void extractCorrelationId(Exchange exchange, String logEvent) {
        String cid = exchange.getIn().getHeader(KafkaConstants.KEY, String.class);
        if (cid == null || cid.isBlank()) {
            cid = UUID.randomUUID().toString();
        }
        exchange.setProperty(LogConstants.PROP_CORRELATION_ID, cid);
        exchange.getIn().setHeader("X-Correlation-ID", cid);
        log.info(logEvent, cid,
            "topic",  exchange.getIn().getHeader(KafkaConstants.TOPIC),
            "offset", exchange.getIn().getHeader(KafkaConstants.OFFSET));
    }

    /**
     * Manually commits the Kafka consumer offset after message processing completes
     * (success, retry-queue, or DLQ) to prevent infinite redelivery.
     */
    protected void commitOffset(Exchange exchange) {
        KafkaManualCommit commit = exchange.getIn().getHeader(
            KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        String cid = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        if (commit != null) {
            commit.commit();
            log.debug("kafkaOffsetCommitted", cid);
        } else {
            log.warn("kafkaManualCommitHeaderMissing", cid);
        }
    }

    /**
     * Sets exchange properties required by {@link KafkaErrorRoutes} before routing
     * to {@code direct:publishToRetryQueue} or {@code direct:publishToDlq}.
     */
    protected void setErrorProps(Exchange exchange, String topic,
                                  String destination, String serviceName) {
        int attempt = exchange.getIn().getHeader(Exchange.REDELIVERY_COUNTER, 0, Integer.class) + 1;
        exchange.setProperty(LogConstants.PROP_SERVICE_NAME,  serviceName);
        exchange.setProperty(LogConstants.PROP_RETRY_ATTEMPT, attempt);
        if ("RETRY".equals(destination)) {
            exchange.setProperty(LogConstants.PROP_RETRY_TOPIC, topic);
        } else {
            exchange.setProperty(LogConstants.PROP_DLQ_TOPIC, topic);
        }
        log.warn("kafkaMessageRouting",
            exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
            "destination", destination, "topic", topic,
            "attempt", attempt, "serviceName", serviceName);
    }
}
