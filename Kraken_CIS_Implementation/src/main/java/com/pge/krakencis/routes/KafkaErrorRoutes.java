package com.pge.krakencis.routes;

import com.pge.krakencis.configs.KafkaProducerConfig;
import com.pge.krakencis.exceptions.ExternalServiceException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Camel routes that route failed Kafka consumer messages to the retry queue
 * or the Dead Letter Queue (DLQ).
 *
 * <h3>Message contract</h3>
 * <p>The Kafka record published to the retry / DLQ topic preserves the
 * <b>original message body exactly as it was consumed</b> from the source topic —
 * no wrapper object, no reformatting. Error context is attached as
 * <b>Kafka record headers</b> so consumers and monitoring tools can inspect
 * failures without parsing the payload.
 *
 * <h3>Kafka record headers set on every error message</h3>
 * <pre>
 * X-Destination-Type   RETRY | DLQ
 * X-Correlation-ID     end-to-end trace ID
 * X-Service-Name       downstream service that failed
 * X-Error-Type         Java exception simple name
 * X-Error-Message      exception message
 * X-Error-Http-Status  HTTP status code (0 = connection failure)
 * X-Error-Attempt      attempt number on final failure
 * X-Error-Timestamp    ISO-8601 timestamp of the failure
 * </pre>
 *
 * <h3>Callers must set these exchange properties before routing here</h3>
 * <ul>
 *   <li>{@link LogConstants#PROP_ORIGINAL_BODY}  — raw Kafka message body (String)</li>
 *   <li>{@link LogConstants#PROP_SERVICE_NAME}   — downstream service name</li>
 *   <li>{@link LogConstants#PROP_RETRY_ATTEMPT}  — attempt number on final failure</li>
 *   <li>{@link LogConstants#PROP_RETRY_TOPIC}    — retry topic name (retry route only)</li>
 *   <li>{@link LogConstants#PROP_DLQ_TOPIC}      — DLQ topic name (DLQ route only)</li>
 *   <li>{@link Exchange#EXCEPTION_CAUGHT}        — set automatically by Camel</li>
 * </ul>
 */
@Component
public class KafkaErrorRoutes extends RouteBuilder {

    private static final StructuredLogger log = StructuredLogger.of(KafkaErrorRoutes.class);

    private final KafkaProducerConfig kafkaProducerConfig;

    public KafkaErrorRoutes(KafkaProducerConfig kafkaProducerConfig) {
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    @Override
    public void configure() {
        final String kafkaQuery = kafkaProducerConfig.buildQueryString();

        // ── Retry queue ───────────────────────────────────────────────────────
        // Body  = original Kafka message (unchanged)
        // Headers = error metadata for observability
        from("direct:publishToRetryQueue")
            .routeId("route-publish-retry-queue")
            .process(exchange -> prepareErrorMessage(exchange, "RETRY"))
            .toD("kafka:${exchangeProperty." + LogConstants.PROP_RETRY_TOPIC + "}?" + kafkaQuery)
            .process(exchange -> log.warn("messageRoutedToRetryQueue",
                exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                "retryTopic", exchange.getProperty(LogConstants.PROP_RETRY_TOPIC, String.class)));

        // ── Dead Letter Queue ─────────────────────────────────────────────────
        // Body  = original Kafka message (unchanged — ready for replay or inspection)
        // Headers = error metadata for observability / alerting
        from("direct:publishToDlq")
            .routeId("route-publish-dlq")
            .process(exchange -> prepareErrorMessage(exchange, "DLQ"))
            .toD("kafka:${exchangeProperty." + LogConstants.PROP_DLQ_TOPIC + "}?" + kafkaQuery)
            .process(exchange -> log.error("messageRoutedToDlq",
                exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class),
                "dlqTopic", exchange.getProperty(LogConstants.PROP_DLQ_TOPIC, String.class)));
    }

    // ─────────────────────────────────────────────────────────────────────────

    private void prepareErrorMessage(Exchange exchange, String destinationType) {
        String    correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String    originalBody  = exchange.getProperty(LogConstants.PROP_ORIGINAL_BODY,  String.class);
        String    serviceName   = exchange.getProperty(LogConstants.PROP_SERVICE_NAME,   String.class);
        int       attempt       = exchange.getProperty(LogConstants.PROP_RETRY_ATTEMPT,  0, Integer.class);
        Exception ex            = exchange.getProperty(Exchange.EXCEPTION_CAUGHT,        Exception.class);

        int httpStatus = 0;
        if (ex instanceof ExternalServiceException ese && ese.getHttpStatusCode() != null) {
            httpStatus = ese.getHttpStatusCode();
        }

        // ── Body: original Kafka message body, untouched ──────────────────────
        exchange.getIn().setBody(originalBody != null ? originalBody : "");

        // ── Kafka message key ─────────────────────────────────────────────────
        exchange.getIn().setHeader(KafkaConstants.KEY, correlationId);

        // ── Kafka record headers: error metadata ──────────────────────────────
        exchange.getIn().setHeader("X-Destination-Type",  destinationType);
        exchange.getIn().setHeader("X-Correlation-ID",    correlationId);
        exchange.getIn().setHeader("X-Service-Name",      serviceName);
        exchange.getIn().setHeader("X-Error-Type",
            ex != null ? ex.getClass().getSimpleName() : "Unknown");
        exchange.getIn().setHeader("X-Error-Message",
            ex != null ? ex.getMessage() : "Unknown error");
        exchange.getIn().setHeader("X-Error-Http-Status", String.valueOf(httpStatus));
        exchange.getIn().setHeader("X-Error-Attempt",     String.valueOf(attempt));
        exchange.getIn().setHeader("X-Error-Timestamp",
            OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));

        // Preserve the DLQ replay counter across the retry/DLQ round trip. X-Replay-Count is
        // set by DlqReplayRoute when it replays a message to the source topic, and it is the
        // ONLY cap that stops replay (→ parking). Unlike X-Error-Attempt it is not otherwise
        // re-stamped, so without this it is lost on the consume→retry→DLQ journey, the replayer
        // always reads 0, and the message is replayed forever and never parks. Re-stamp it
        // explicitly (byte[]-safe, since Kafka delivers headers as byte[]).
        Object rawReplayCount = exchange.getIn().getHeader("X-Replay-Count");
        if (rawReplayCount != null) {
            String replayCount = (rawReplayCount instanceof byte[] bytes)
                ? new String(bytes, StandardCharsets.UTF_8)
                : rawReplayCount.toString();
            exchange.getIn().setHeader("X-Replay-Count", replayCount);
        }
    }
}
