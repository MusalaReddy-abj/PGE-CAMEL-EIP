package com.pge.krakencis.routes;

import com.pge.krakencis.exceptions.ExternalServiceException;
import com.pge.krakencis.exceptions.RetryQueueExhaustedException;
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
 * <h3>Error handler decision tree</h3>
 * <pre>
 * Exception thrown
 *   ├─ ValidationException / TransformationException  → DLQ immediately
 *   ├─ ExternalServiceException (4xx)                 → DLQ immediately
 *   ├─ RetryQueueExhaustedException                   → final DLQ (retry-queue limit exceeded)
 *   ├─ RetryableException  → retry 3× (1s→2s→4s) → retry queue
 *   └─ Exception (unknown) → retry 3× (1s→2s→4s) → retry queue
 * </pre>
 *
 * <h3>X-Error-Attempt accumulation</h3>
 * {@link #setErrorProps} reads the existing {@code X-Error-Attempt} Kafka header
 * from the incoming message (set by the previous pass through this route) and
 * adds the current Camel in-process retry count. This ensures the counter
 * accumulates correctly across retry-queue re-entries:
 * <pre>
 * Main consumer fails after 3 retries  → X-Error-Attempt: 3
 * Retry consumer fails after 3 retries → X-Error-Attempt: 6
 * Retry consumer fails after 3 retries → X-Error-Attempt: 9
 * </pre>
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
     * Wires five {@code onException} handlers onto {@code route} in priority order:
     *
     * <ol>
     *   <li>Payload errors ({@link ValidationException}, {@link TransformationException})
     *       → DLQ immediately — payload is permanently invalid.</li>
     *   <li>Client errors ({@link ExternalServiceException}) → DLQ immediately.</li>
     *   <li>{@link RetryQueueExhaustedException} → final DLQ — retry-queue limit exceeded,
     *       no more re-attempts.</li>
     *   <li>{@link RetryableException} (5xx / 404 / 408 / 429 / network) → retry 3×
     *       (1 s → 2 s → 4 s) → retry queue.</li>
     *   <li>{@link Exception} catch-all (possibly transient) → retry 3× → retry queue.</li>
     * </ol>
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

        // Retry-queue limit exceeded → final DLQ, no more re-attempts
        route.onException(RetryQueueExhaustedException.class)
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

    /**
     * Error handlers for <b>retry-queue consumers</b> — stricter than
     * {@link #configureKafkaErrorHandlers}.
     *
     * <p>The key difference: the {@code Exception} catch-all routes to
     * <b>DLQ</b> instead of back to the retry queue. A message already in the
     * retry queue has had its transient chances; any unexpected error at this
     * stage is treated as permanent.
     *
     * <p>Only {@link RetryableException} (confirmed transient: 5xx, 404, 408,
     * 429, network) is allowed to re-enter the retry queue.
     *
     * <p>Invalid payloads ({@link ValidationException},
     * {@link TransformationException}) are <b>never</b> sent to the retry queue —
     * they go to DLQ immediately regardless of which consumer catches them.
     */
    protected void configureRetryQueueErrorHandlers(RouteDefinition route,
                                                     String retryTopic,
                                                     String dlqTopic,
                                                     String serviceName) {
        // Payload errors → DLQ immediately (invalid payload — never retry)
        route.onException(ValidationException.class, TransformationException.class)
            .handled(true)
            .process(exchange -> setErrorProps(exchange, dlqTopic, "DLQ", serviceName))
            .to("direct:publishToDlq")
            .process(this::commitOffset)
        .end();

        // 4xx client error → DLQ (permanent — downstream rejected the request)
        route.onException(ExternalServiceException.class)
            .handled(true)
            .process(exchange -> setErrorProps(exchange, dlqTopic, "DLQ", serviceName))
            .to("direct:publishToDlq")
            .process(this::commitOffset)
        .end();

        // Retry-queue limit exceeded → final DLQ
        route.onException(RetryQueueExhaustedException.class)
            .handled(true)
            .process(exchange -> setErrorProps(exchange, dlqTopic, "DLQ", serviceName))
            .to("direct:publishToDlq")
            .process(this::commitOffset)
        .end();

        // Transient service errors → retry 3× within this pass → retry queue
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

        // Any other unexpected error in retry consumer → DLQ (not retry queue)
        // The message is already on its second chance; unknown errors are treated as permanent.
        route.onException(Exception.class)
            .handled(true)
            .process(exchange -> setErrorProps(exchange, dlqTopic, "DLQ", serviceName))
            .to("direct:publishToDlq")
            .process(this::commitOffset)
        .end();
    }

    // ── Shared helper methods ─────────────────────────────────────────────────

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
     * Prepares exchange properties for {@link KafkaErrorRoutes}.
     *
     * <p><b>Attempt accumulation:</b> reads {@code X-Error-Attempt} from the incoming
     * Kafka record headers (written by a previous retry-queue pass) and adds the
     * current Camel in-process redelivery count. This ensures the counter grows
     * monotonically across retry-queue re-entries so retry-queue consumers can
     * correctly detect when the limit is reached.
     */
    protected void setErrorProps(Exchange exchange, String topic,
                                  String destination, String serviceName) {
        // Previous accumulated attempt count (from X-Error-Attempt Kafka header)
        int prevAttempt = 0;
        String prevStr = exchange.getIn().getHeader("X-Error-Attempt", String.class);
        if (prevStr != null) {
            try { prevAttempt = Integer.parseInt(prevStr); } catch (NumberFormatException ignored) {}
        }

        // In-process Camel redeliveries for this pass (0 = first attempt, 3 = third retry)
        int camelRetries = exchange.getIn().getHeader(Exchange.REDELIVERY_COUNTER, 0, Integer.class);

        // Total = all previous attempts + this pass's attempts
        int totalAttempt = prevAttempt + camelRetries + 1;

        exchange.setProperty(LogConstants.PROP_SERVICE_NAME,  serviceName);
        exchange.setProperty(LogConstants.PROP_RETRY_ATTEMPT, totalAttempt);
        if ("RETRY".equals(destination)) {
            exchange.setProperty(LogConstants.PROP_RETRY_TOPIC, topic);
        } else {
            exchange.setProperty(LogConstants.PROP_DLQ_TOPIC, topic);
        }
        log.warn("kafkaMessageRouting",
            exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
            "destination", destination, "topic", topic,
            "totalAttempt", totalAttempt, "prevAttempt", prevAttempt,
            "camelRetries", camelRetries, "serviceName", serviceName);
    }
}
