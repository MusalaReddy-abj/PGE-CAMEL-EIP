package com.pge.krakencis.observability;

import com.pge.krakencis.logging.LogConstants;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import org.apache.camel.Exchange;

/**
 * OpenTelemetry Integration — stamps the standard span attribute set from a Camel exchange.
 *
 * <p>Reads correlation/business identifiers that the existing processors already place on the
 * exchange (it never computes or mutates them) and writes them onto the span as attributes:
 * {@code correlation.id}, {@code business.identifier}, {@code route.id}, {@code operation},
 * {@code service.name}, {@code kafka.topic}. Null-safe and read-only — no business logic changes.
 */
public final class SpanAttributeHelper {

    private SpanAttributeHelper() {}

    /** Configured native service name; set once by {@link OpenTelemetryConfig}. */
    private static volatile String serviceName = "camel-rcd-service-dev-main";

    static void setServiceName(String name) {
        if (name != null && !name.isBlank()) {
            serviceName = name;
        }
    }

    /** Stamps the standard attributes onto {@code span}, reading values from {@code exchange}. */
    public static void stamp(Span span, Exchange exchange) {
        if (span == null || !span.getSpanContext().isValid() || exchange == null) {
            return;
        }

        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        if (correlationId == null) {
            correlationId = exchange.getIn().getHeader("X-Correlation-ID", String.class);
        }
        put(span, TracingConstants.ATTR_CORRELATION_ID, correlationId);

        put(span, TracingConstants.ATTR_ROUTE_ID, exchange.getFromRouteId());
        put(span, TracingConstants.ATTR_KAFKA_TOPIC,
                exchange.getProperty(LogConstants.KAFKA_TOPIC, String.class));

        // business.identifier — the RCDC meter mRID set by RcdcRequestProcessor (best-effort).
        Object mRID = exchange.getProperty("rcdc.mRID");
        if (mRID != null) {
            put(span, TracingConstants.ATTR_BUSINESS_IDENTIFIER, mRID.toString());
        }

        span.setAttribute(TracingConstants.ATTR_SERVICE_NAME, serviceName);
    }

    /** Adds an explicit operation attribute (route operation name). */
    public static void setOperation(Span span, String operation) {
        put(span, TracingConstants.ATTR_OPERATION, operation);
    }

    private static void put(Span span, AttributeKey<String> key, String value) {
        if (value != null && !value.isBlank()) {
            span.setAttribute(key, value);
        }
    }
}
