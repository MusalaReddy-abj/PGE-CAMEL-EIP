package com.pge.krakencis.routes;

import com.pge.krakencis.exceptions.ExternalServiceException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.OutboundRequestEvent;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Reusable Camel routes for publishing failed Kafka consumer messages to either
 * a retry queue or the Dead Letter Queue (DLQ).
 *
 * <h3>Callers must set these exchange properties before routing here:</h3>
 * <ul>
 *   <li>{@link LogConstants#PROP_ORIGINAL_BODY}  — original Kafka message body (String)</li>
 *   <li>{@link LogConstants#PROP_SERVICE_NAME}   — downstream service name</li>
 *   <li>{@link LogConstants#PROP_RETRY_ATTEMPT}  — attempt number on final failure</li>
 *   <li>{@link LogConstants#PROP_RETRY_TOPIC}    — Kafka retry topic name (for retry route)</li>
 *   <li>{@link LogConstants#PROP_DLQ_TOPIC}      — Kafka DLQ topic name (for DLQ route)</li>
 *   <li>{@link Exchange#EXCEPTION_CAUGHT}        — the caught exception (set by Camel)</li>
 * </ul>
 *
 * <h3>Routes provided:</h3>
 * <ul>
 *   <li>{@code direct:publishToRetryQueue} — transient failure; consumer should re-attempt later</li>
 *   <li>{@code direct:publishToDlq}        — permanent failure; requires manual investigation</li>
 * </ul>
 */
@Component
public class KafkaErrorRoutes extends RouteBuilder {

    private static final StructuredLogger log = StructuredLogger.of(KafkaErrorRoutes.class);
    private static final int              MAX_ATTEMPTS = 3;

    @Override
    public void configure() {

        from("direct:publishToRetryQueue")
            .routeId("route-publish-retry-queue")
            .process(exchange -> buildErrorEvent(exchange, "RETRY"))
            .process(exchange -> exchange.setProperty(LogConstants.KAFKA_TOPIC,
                                                       exchange.getProperty(LogConstants.PROP_RETRY_TOPIC)))
            .process(exchange -> exchange.setProperty(LogConstants.KAFKA_KEY,
                                                       exchange.getProperty(LogConstants.PROP_CORRELATION_ID)))
            .to("direct:publishToKafka")
            .process(exchange -> {
                String cid   = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
                String topic = exchange.getProperty(LogConstants.PROP_RETRY_TOPIC, String.class);
                log.warn("messageRoutedToRetryQueue", cid, "retryTopic", topic);
            });

        from("direct:publishToDlq")
            .routeId("route-publish-dlq")
            .process(exchange -> buildErrorEvent(exchange, "DLQ"))
            .process(exchange -> exchange.setProperty(LogConstants.KAFKA_TOPIC,
                                                       exchange.getProperty(LogConstants.PROP_DLQ_TOPIC)))
            .process(exchange -> exchange.setProperty(LogConstants.KAFKA_KEY,
                                                       exchange.getProperty(LogConstants.PROP_CORRELATION_ID)))
            .to("direct:publishToKafka")
            .process(exchange -> {
                String cid   = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
                String topic = exchange.getProperty(LogConstants.PROP_DLQ_TOPIC, String.class);
                log.error("messageRoutedToDlq", cid, "dlqTopic", topic);
            });
    }

    private void buildErrorEvent(Exchange exchange, String eventType) {
        String    correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String    originalBody  = exchange.getProperty(LogConstants.PROP_ORIGINAL_BODY,  String.class);
        String    serviceName   = exchange.getProperty(LogConstants.PROP_SERVICE_NAME,   String.class);
        int       attempt       = exchange.getProperty(LogConstants.PROP_RETRY_ATTEMPT,  0, Integer.class);
        Exception ex            = exchange.getProperty(Exchange.EXCEPTION_CAUGHT,        Exception.class);

        int httpStatus = 0;
        if (ex instanceof ExternalServiceException ese && ese.getHttpStatusCode() != null) {
            httpStatus = ese.getHttpStatusCode();
        }

        OutboundRequestEvent event = OutboundRequestEvent.builder()
            .eventType(eventType)
            .correlationId(correlationId)
            .serviceName(serviceName)
            .body(originalBody)
            .attempt(attempt)
            .maxAttempts(MAX_ATTEMPTS)
            .httpStatus(httpStatus)
            .errorType(ex != null ? ex.getClass().getSimpleName() : "Unknown")
            .errorMessage(ex != null ? ex.getMessage()            : "Unknown error")
            .timestamp(OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
            .build();

        exchange.getIn().setBody(event);
    }
}
