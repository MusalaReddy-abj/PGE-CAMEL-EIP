package com.pge.krakencis.logging;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.apache.camel.Exchange;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Manages the SLF4J MDC (Mapped Diagnostic Context) and OTel span lifecycle
 * for every Camel exchange.
 *
 * <p>Call {@link #populateMDC(Exchange)} at route entry to:
 * <ul>
 *   <li>Stamp log lines with correlationId, routeId, exchangeId.</li>
 *   <li>Start a Micrometer Tracing span — the OTel bridge then automatically
 *       injects {@code traceId} and {@code spanId} into MDC for every log line
 *       within the route.</li>
 * </ul>
 *
 * <p>Call {@link #endTrace(Exchange)} followed by {@link #clearMDC()} at route
 * exit to close the span (so it is exported) and clear all MDC keys (prevents
 * leakage when the thread is reused from a pool).
 *
 * <h3>Why spans are needed</h3>
 * <p>Camel's Undertow REST server (port 8122) is not a Spring MVC endpoint.
 * Micrometer Tracing only auto-instruments Spring MVC — so without manually
 * starting a span here, {@code traceId}/{@code spanId} in MDC would always be
 * empty.
 */
@Component
public class MDCContextManager {

    private static final int CORRELATION_ID_MAX_LEN = 128;

    /** Null-safe: present when micrometer-tracing-bridge-otel is on the classpath. */
    @Autowired(required = false)
    private Tracer tracer;

    public String initCorrelationId(Exchange exchange) {
        String correlationId = exchange.getIn().getHeader(
            LogConstants.PROP_CORRELATION_ID, String.class);

        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        } else {
            correlationId = sanitize(correlationId);
        }

        exchange.setProperty(LogConstants.PROP_CORRELATION_ID, correlationId);
        exchange.getIn().setHeader(LogConstants.PROP_CORRELATION_ID, correlationId);
        return correlationId;
    }

    /** Strips control characters (prevents log injection) and enforces a max length. */
    private static String sanitize(String value) {
        String cleaned = value.replaceAll("[\\p{Cntrl}]", "_");
        return cleaned.length() > CORRELATION_ID_MAX_LEN
            ? cleaned.substring(0, CORRELATION_ID_MAX_LEN)
            : cleaned;
    }

    public void populateMDC(Exchange exchange) {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID,
                                                     String.class);
        if (correlationId == null) {
            correlationId = initCorrelationId(exchange);
        }

        MDC.put(LogConstants.MDC_CORRELATION_ID, correlationId);
        MDC.put(LogConstants.MDC_EXCHANGE_ID,    exchange.getExchangeId());

        if (exchange.getFromRouteId() != null) {
            MDC.put(LogConstants.MDC_ROUTE_ID, exchange.getFromRouteId());
        }

        startSpan(exchange);
    }

    /**
     * Closes the OTel span scope and ends the span that was started in
     * {@link #populateMDC(Exchange)}. Must be called before {@link #clearMDC()}
     * so the final audit log line still carries traceId/spanId.
     */
    public void endTrace(Exchange exchange) {
        if (tracer == null) return;

        Tracer.SpanInScope scope = exchange.getProperty(
            LogConstants.PROP_OTEL_SCOPE, Tracer.SpanInScope.class);
        Span span = exchange.getProperty(
            LogConstants.PROP_OTEL_SPAN, Span.class);

        // Closing the scope removes traceId/spanId from MDC (done by OTel bridge)
        if (scope != null) {
            scope.close();
            exchange.removeProperty(LogConstants.PROP_OTEL_SCOPE);
        }
        // end() ships the span to the configured exporter
        if (span != null) {
            span.end();
            exchange.removeProperty(LogConstants.PROP_OTEL_SPAN);
        }
    }

    public void clearMDC() {
        MDC.remove(LogConstants.MDC_CORRELATION_ID);
        MDC.remove(LogConstants.MDC_EXCHANGE_ID);
        MDC.remove(LogConstants.MDC_ROUTE_ID);
        MDC.remove(LogConstants.MDC_MESSAGE_TYPE);
        MDC.remove(LogConstants.MDC_SOURCE_SYSTEM);
        MDC.remove(LogConstants.MDC_TARGET_SYSTEM);
        // Safety fallback: remove trace keys in case endTrace() was not called
        // (e.g. on error paths that bypass routeLoggingProcessor.exit)
        MDC.remove(LogConstants.MDC_TRACE_ID);
        MDC.remove(LogConstants.MDC_SPAN_ID);
    }

    public void set(String key, String value) {
        if (value != null) {
            MDC.put(key, value);
        }
    }

    // ── private ───────────────────────────────────────────────────────────────

    private void startSpan(Exchange exchange) {
        if (tracer == null) return;

        String routeId = exchange.getFromRouteId();
        String spanName = routeId != null ? "camel." + routeId : "camel.route";

        Span span = tracer.nextSpan().name(spanName).start();
        // withSpan() makes this span the current span on this thread.
        // The OTel bridge writes traceId + spanId into MDC immediately.
        Tracer.SpanInScope scope = tracer.withSpan(span);

        exchange.setProperty(LogConstants.PROP_OTEL_SPAN,  span);
        exchange.setProperty(LogConstants.PROP_OTEL_SCOPE, scope);
    }
}
