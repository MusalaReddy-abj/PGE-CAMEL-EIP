package com.pge.krakencis.logging;

import org.apache.camel.Exchange;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Manages the SLF4J MDC (Mapped Diagnostic Context) for every Camel exchange.
 *
 * <p>Call {@link #populateMDC(Exchange)} at route entry to stamp log lines with
 * correlationId, routeId and exchangeId. Call {@link #clearMDC()} at route exit
 * to clear all MDC keys (prevents leakage when the thread is reused from a pool).
 *
 * <h3>OpenTelemetry Java Agent Migration</h3>
 * <p>Native SDK removed / Instrumentation provided by Java Agent. Span creation is
 * no longer managed here — the OpenTelemetry Java Agent creates the entry span (and
 * for Camel routes/Undertow REST) automatically and injects {@code trace_id} /
 * {@code span_id} into MDC via its logback-mdc instrumentation. The former
 * Micrometer-Tracing {@code Tracer}-based span lifecycle has been removed;
 * {@link #endTrace(Exchange)} is retained as a safe no-op for callers.
 */
@Component
public class MDCContextManager {

    private static final int CORRELATION_ID_MAX_LEN = 128;

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
    }

    /**
     * OpenTelemetry Java Agent Migration — Native SDK removed / Instrumentation
     * provided by Java Agent. The Agent owns the span lifecycle, so there is no
     * application-managed span to close here. Retained as a safe no-op so existing
     * callers ({@link RouteLoggingProcessor#cleanup(Exchange)} and onException
     * handlers) need no changes.
     */
    public void endTrace(Exchange exchange) {
        // No-op: span lifecycle is handled by the OpenTelemetry Java Agent.
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
}
