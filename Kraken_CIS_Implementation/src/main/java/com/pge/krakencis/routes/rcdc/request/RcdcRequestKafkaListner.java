package com.pge.krakencis.routes.rcdc.request;

import com.pge.krakencis.exceptions.RetryableException;
import com.pge.krakencis.exceptions.TransformationException;
import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.rcdc.request.RcdcTargetCallProcessor;
import com.pge.krakencis.processors.rcdc.request.RcdcTargetResponseProcessor;
import com.pge.krakencis.routes.BaseRoute;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Kafka consumer route for inbound RCDC (Remote Connect/Disconnect Command) events.
 *
 * <h3>Retry policy</h3>
 * <ul>
 *   <li>Payload errors ({@link ValidationException}, {@link TransformationException})
 *       → DLQ immediately, no retry.</li>
 *   <li>Transient errors ({@link RetryableException}: 5xx, 408, 429, network)
 *       → retry 3× (1 s / 2 s / 4 s) → retry topic on exhaustion.</li>
 *   <li>All other errors → DLQ.</li>
 * </ul>
 * Kafka offset is committed in every path (success, retry-exhausted, DLQ).
 *
 * <h3>Route ID</h3>
 * {@code route-rcdc-kafka-consumer}
 */
@Component
public class RcdcRequestKafkaListner extends BaseRoute {

    private static final StructuredLogger log          = StructuredLogger.of(RcdcRequestKafkaListner.class);
    private static final String           OPERATION    = "consumeRcdcRequest";
    private static final String           SERVICE_NAME = "SOA-RCDC-TargetService";
    private static final int              MAX_RETRIES  = 3;

    private final RcdcTargetCallProcessor     rcdcTargetCallProcessor;
    private final RcdcTargetResponseProcessor rcdcTargetResponseProcessor;

    @Value("${kafka.topic.rcdc-retry:kraken-rcdc-retry-events}")
    private String rcdcRetryTopic;

    @Value("${kafka.topic.rcdc-dlq:kraken-rcdc-dlq-events}")
    private String rcdcDlqTopic;

    public RcdcRequestKafkaListner(CorrelationIdProcessor      correlationIdProcessor,
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
        final String uri =
            "kafka:{{kafka.topic.rcdc:kraken-rcdc-events}}"
            + "?brokers={{kafka.producer.brokers}}"
            + "&groupId={{kafka.consumer.group-id:kraken-cis-group}}"
            + "&autoOffsetReset={{kafka.consumer.auto-offset-reset:earliest}}"
            + "&maxPollRecords={{kafka.consumer.max-poll-records:500}}"
            + "&autoCommitEnable=false&allowManualCommit=true";

        from(uri).routeId("route-rcdc-kafka-consumer")

            // ── Payload errors → DLQ immediately (no retry) ──────────────────
            .onException(ValidationException.class, TransformationException.class)
                .handled(true)
                .process(exchange -> setErrorProps(exchange, rcdcDlqTopic, "DLQ"))
                .to("direct:publishToDlq")
                .process(this::commitOffset)
            .end()

            // ── Transient failures → retry 3× → retry queue ──────────────────
            .onException(RetryableException.class)
                .maximumRedeliveries(MAX_RETRIES)
                .redeliveryDelay(1_000)
                .backOffMultiplier(2)
                .useExponentialBackOff()
                .retryAttemptedLogLevel(LoggingLevel.WARN)
                .handled(true)
                .process(exchange -> setErrorProps(exchange, rcdcRetryTopic, "RETRY"))
                .to("direct:publishToRetryQueue")
                .process(this::commitOffset)
            .end()

            // ── All other errors → DLQ ────────────────────────────────────────
            .onException(Exception.class)
                .handled(true)
                .process(exchange -> setErrorProps(exchange, rcdcDlqTopic, "DLQ"))
                .to("direct:publishToDlq")
                .process(this::commitOffset)
            .end()

            // ── Happy path ────────────────────────────────────────────────────
            .process(exchange -> exchange.setProperty(
                LogConstants.PROP_ORIGINAL_BODY, exchange.getIn().getBody(String.class)))
            .process(this::extractCorrelationId)
            .process(routeLoggingProcessor.entry(OPERATION))
            .process(rcdcTargetCallProcessor)
            .process(rcdcTargetResponseProcessor)
            .process(this::commitOffset)
            .process(routeLoggingProcessor.exit(OPERATION));
    }

    private void setErrorProps(Exchange exchange, String topic, String destination) {
        int attempt = exchange.getIn().getHeader(Exchange.REDELIVERY_COUNTER, 0, Integer.class) + 1;
        exchange.setProperty(LogConstants.PROP_SERVICE_NAME,  SERVICE_NAME);
        exchange.setProperty(LogConstants.PROP_RETRY_ATTEMPT, attempt);
        if ("RETRY".equals(destination)) {
            exchange.setProperty(LogConstants.PROP_RETRY_TOPIC, topic);
        } else {
            exchange.setProperty(LogConstants.PROP_DLQ_TOPIC, topic);
        }
        log.warn("rcdcKafkaMessageRouting",
            exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
            "destination", destination, "topic", topic, "attempt", attempt);
    }

    private void extractCorrelationId(Exchange exchange) {
        String cid = exchange.getIn().getHeader(KafkaConstants.KEY, String.class);
        if (cid == null || cid.isBlank()) {
            cid = UUID.randomUUID().toString();
        }
        exchange.setProperty(LogConstants.PROP_CORRELATION_ID, cid);
        exchange.getIn().setHeader("X-Correlation-ID", cid);
        log.info("rcdcMessageConsumed", cid,
            "topic",  exchange.getIn().getHeader(KafkaConstants.TOPIC),
            "offset", exchange.getIn().getHeader(KafkaConstants.OFFSET));
    }

    private void commitOffset(Exchange exchange) {
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
}
