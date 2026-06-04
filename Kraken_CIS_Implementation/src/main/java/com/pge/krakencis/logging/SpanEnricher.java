package com.pge.krakencis.logging;

import io.opentelemetry.api.trace.Span;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaConstants;

/**
 * Enriches the active OpenTelemetry span for inbound REST requests.
 *
 * <p>The REST layer is served by Camel's {@code undertow} component (see
 * {@code RestApiConfig}), not Spring MVC. Spring Boot's automatic HTTP server
 * instrumentation only covers Spring MVC, so the inbound span produced by
 * camel-opentelemetry has no {@code http.route} attribute and a generic name —
 * the request path does not show up in traces.
 *
 * <p>This helper stamps the (low-cardinality) route template onto the current
 * span as {@code http.route} and renames the span to {@code "METHOD /path"} so
 * traces in Tempo/Jaeger group and read correctly. It is a no-op when no valid
 * span is active, so it is always safe to call.
 */
public final class SpanEnricher {

    private SpanEnricher() {}

    /**
     * Returns a processor that sets {@code http.route} and the span name from the
     * given static route template.
     *
     * @param httpMethod e.g. {@code POST}, {@code GET}
     * @param routePath  the low-cardinality path template, e.g. {@code /api/v1/rcdc}
     */
    public static Processor httpRoute(String httpMethod, String routePath) {
        return exchange -> {
            Span span = Span.current();
            if (span == null || !span.getSpanContext().isValid()) {
                return; // no active/valid span (tracing off or not started) — nothing to enrich
            }
            span.setAttribute("http.route",  routePath);
            span.setAttribute("http.request.method", httpMethod);
            span.updateName(httpMethod + " " + routePath);
        };
    }

    /**
     * Returns a processor that stamps OTel semantic-convention messaging attributes
     * onto the active span for a Kafka <b>consumer</b> route, and names the span
     * {@code "process <topic>"}. The topic is read per-message from the
     * {@link KafkaConstants#TOPIC} header, so it reflects the actual source topic.
     *
     * <p>This is the Kafka analog of {@link #httpRoute}: without it the consumer
     * span produced by camel-opentelemetry has a generic name and no
     * {@code messaging.destination.name}, so traces can't be grouped by topic.
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
