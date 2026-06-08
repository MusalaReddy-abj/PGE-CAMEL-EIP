package com.pge.krakencis.observability;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.camel.Exchange;
import org.apache.camel.Message;

import java.nio.charset.StandardCharsets;

/**
 * OpenTelemetry Integration — W3C trace-context propagation across Camel message boundaries.
 *
 * <p>Injects the active trace context into the exchange IN-message headers before a Kafka
 * publish (Camel maps headers to Kafka record headers), and extracts it on consume so the
 * consumer span joins the producer's trace. All operations are additive — they only read/write
 * the {@code traceparent} header and never touch the message body or existing headers. Null-safe.
 */
public final class ContextPropagationHelper {

    private ContextPropagationHelper() {}

    /** Writes header values onto the Camel IN message. */
    private static final TextMapSetter<Message> SETTER = (message, key, value) -> {
        if (message != null) {
            message.setHeader(key, value);
        }
    };

    /** Reads header values from the Camel IN message, tolerating {@code byte[]} (Kafka wire form). */
    private static final TextMapGetter<Message> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Message carrier) {
            return carrier.getHeaders().keySet();
        }

        @Override
        public String get(Message carrier, String key) {
            if (carrier == null) {
                return null;
            }
            Object value = carrier.getHeader(key);
            if (value == null) {
                return null;
            }
            if (value instanceof byte[] bytes) {
                return new String(bytes, StandardCharsets.UTF_8);
            }
            return value.toString();
        }
    };

    /** Injects {@code Context.current()} (the active trace) into the IN-message headers. */
    public static void injectIntoHeaders(Exchange exchange) {
        OpenTelemetry otel = TracingHelper.openTelemetry();
        if (otel == null || exchange == null) {
            return;
        }
        otel.getPropagators().getTextMapPropagator()
                .inject(Context.current(), exchange.getIn(), SETTER);
    }

    /**
     * Extracts the trace context carried in the IN-message headers, falling back to
     * {@code Context.current()} when no {@code traceparent} is present (or tracing is disabled).
     */
    public static Context extractFromHeaders(Exchange exchange) {
        OpenTelemetry otel = TracingHelper.openTelemetry();
        if (otel == null || exchange == null) {
            return Context.current();
        }
        return otel.getPropagators().getTextMapPropagator()
                .extract(Context.current(), exchange.getIn(), GETTER);
    }
}
