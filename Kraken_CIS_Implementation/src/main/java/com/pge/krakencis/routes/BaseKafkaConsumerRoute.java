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
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.model.RouteDefinition;
import org.springframework.beans.factory.annotation.Autowired;

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

    // ── Metric names ─────────────────────────────────────────────────────────
    static final String METRIC_KAFKA_CONSUMED = "kafka.consumed";
    static final String METRIC_KAFKA_DLQ      = "kafka.dlq.published";
    static final String METRIC_KAFKA_RETRY    = "kafka.retry.published";

    // Field-injected to avoid cascading constructor changes across all subclasses.
    @Autowired
    protected MeterRegistry meterRegistry;

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

        // Transient service errors → NO internal retries (the 5-min route delay is the back-off).
        // One attempt per retry-queue read; if it fails, publish back to retry queue (or DLQ
        // if RetryQueueExhaustedException is thrown by the attempt-check processor above).
        route.onException(RetryableException.class)
            .maximumRedeliveries(0)
            .handled(true)
            .process(exchange -> setErrorProps(exchange, retryTopic, "RETRY", serviceName))
            .to("direct:publishToRetryQueue")
            .process(this::commitOffset)
        .end();

        // Any other unexpected error in retry consumer → DLQ (not retry queue).
        // No internal retries — same single-attempt policy as RetryableException above.
        route.onException(Exception.class)
            .maximumRedeliveries(0)
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
        String cid   = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String topic = exchange.getIn().getHeader(KafkaConstants.TOPIC, String.class);
        if (commit != null) {
            commit.commit();
            if (topic != null) {
                meterRegistry.counter(METRIC_KAFKA_CONSUMED,
                    "topic",   topic,
                    "routeId", exchange.getFromRouteId() != null ? exchange.getFromRouteId() : "unknown")
                    .increment();
            }
            log.debug("kafkaOffsetCommitted", cid);
        } else {
            log.warn("kafkaManualCommitHeaderMissing", cid);
        }
    }

    /**
     * Prepares exchange properties for {@link KafkaErrorRoutes}.
     *
     * <p><b>Attempt counting:</b> reads {@code X-Error-Attempt} from the incoming
     * Kafka record header (written by a previous retry-queue pass) and increments
     * it by 1. This counts <em>retry-queue entries</em>, not in-process Camel
     * redeliveries. The retry-queue consumer checks this value against
     * {@code retry.queue.max-attempts} to decide when to send to DLQ.
     *
     * <p>Example with {@code max-attempts=3}:
     * <pre>
     * Main consumer fails (3 in-process retries) → X-Error-Attempt = 1 → retry topic
     * Retry pass 1 fails                          → X-Error-Attempt = 2 → retry topic
     * Retry pass 2 fails                          → X-Error-Attempt = 3 → retry topic
     * Retry pass 3: attempt(3) >= max(3)          → RetryQueueExhaustedException → DLQ
     * </pre>
     */
    protected void setErrorProps(Exchange exchange, String topic,
                                  String destination, String serviceName) {
        // Count only retry-queue entries. Read the previous entry count from the
        // Kafka record header; increment by 1 for this new entry.
        int prevAttempt = parseKafkaHeaderAsInt(exchange, "X-Error-Attempt", 0);
        int totalAttempt = prevAttempt + 1;

        exchange.setProperty(LogConstants.PROP_SERVICE_NAME,  serviceName);
        exchange.setProperty(LogConstants.PROP_RETRY_ATTEMPT, totalAttempt);
        if ("RETRY".equals(destination)) {
            exchange.setProperty(LogConstants.PROP_RETRY_TOPIC, topic);
        } else {
            exchange.setProperty(LogConstants.PROP_DLQ_TOPIC, topic);
        }
        if ("DLQ".equals(destination)) {
            meterRegistry.counter(METRIC_KAFKA_DLQ,
                "topic",       topic,
                "serviceName", serviceName).increment();
        } else {
            meterRegistry.counter(METRIC_KAFKA_RETRY,
                "topic",       topic,
                "serviceName", serviceName).increment();
        }
        log.warn("kafkaMessageRouting",
            exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
            "destination",   destination,
            "topic",         topic,
            "queueEntry",    totalAttempt,
            "prevEntry",     prevAttempt,
            "serviceName",   serviceName);
    }

    /**
     * Reads a Kafka record header as an int, handling both {@code String} and
     * {@code byte[]} types. Kafka record headers are byte arrays on the wire;
     * Camel may deliver them as {@code byte[]} depending on the version and
     * configuration. Falling back to {@code byte[]} parsing prevents the attempt
     * counter from silently resetting to 0 and causing infinite retries.
     */
    protected int parseKafkaHeaderAsInt(Exchange exchange, String headerName, int defaultValue) {
        Object raw = exchange.getIn().getHeader(headerName);
        if (raw == null) return defaultValue;
        if (raw instanceof String s) {
            try { return Integer.parseInt(s.trim()); } catch (NumberFormatException ignored) {}
        }
        if (raw instanceof byte[] bytes) {
            try { return Integer.parseInt(new String(bytes, java.nio.charset.StandardCharsets.UTF_8).trim()); }
            catch (Exception ignored) {}
        }
        return defaultValue;
    }
}
