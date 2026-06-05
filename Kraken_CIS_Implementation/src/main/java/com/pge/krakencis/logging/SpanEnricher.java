package com.pge.krakencis.logging;

import io.opentelemetry.api.trace.Span;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaConstants;

/**
 * Gives inbound REST and Kafka spans meaningful names/attributes.
 *
 * <p>camel-opentelemetry creates a span per route/processor, but the entry-point span
 * is named after the Camel endpoint (generic) and carries no {@code http.route} or
 * {@code messaging.destination}. This helper renames the active span and stamps the
 * OTel semantic-convention attributes so traces read as real operations
 * (e.g. {@code POST /api/v1/rcdc}, {@code process kraken-rcdc-events}). It is a no-op
 * when no valid span is active, so it is always safe to call.
 */
public final class SpanEnricher {

    private SpanEnricher() {}

    /** Stamps {@code http.route} + names the span {@code "METHOD /path"} for a REST route. */
    public static Processor httpRoute(String httpMethod, String routePath) {
        return exchange -> {
            Span span = Span.current();
            if (span == null || !span.getSpanContext().isValid()) {
                return;
            }
            span.setAttribute("http.route", routePath);
            span.setAttribute("http.request.method", httpMethod);
            span.updateName(httpMethod + " " + routePath);
        };
    }

    /**
     * Stamps OTel messaging attributes + names the span {@code "process <topic>"} for a
     * Kafka consumer. The topic is read per-message from the {@link KafkaConstants#TOPIC}
     * header so it reflects the actual source topic.
     */
    public static Processor kafkaConsume() {
        return exchange -> {
            Span span = Span.current();
            if (span == null || !span.getSpanContext().isValid()) {
                return;
            }
            String topic = exchange.getIn().getHeader(KafkaConstants.TOPIC, String.class);
            if (topic == null || topic.isBlank()) {
                return;
            }
            span.setAttribute("messaging.system", "kafka");
            span.setAttribute("messaging.operation", "process");
            span.setAttribute("messaging.destination.name", topic);
            span.updateName("process " + topic);
        };
    }
}
