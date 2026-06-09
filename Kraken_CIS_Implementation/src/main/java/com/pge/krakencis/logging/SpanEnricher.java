package com.pge.krakencis.logging;

import io.opentelemetry.api.trace.Span;
import org.apache.camel.Processor;

/**
 * Gives inbound REST spans meaningful names/attributes.
 *
 * <p>camel-opentelemetry creates a span per route/processor, but the entry-point span
 * is named after the Camel endpoint (generic) and carries no {@code http.route}. This
 * helper renames the active span and stamps the OTel semantic-convention attributes so
 * traces read as real operations (e.g. {@code POST /api/v1/rcdc}). It is a no-op when no
 * valid span is active, so it is always safe to call.
 *
 * <p>Kafka consumer spans are named/attributed (and re-parented onto the producer trace)
 * by {@link KafkaTraceContext} instead.
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
}
