package com.pge.krakencis.logging;

import org.apache.camel.Exchange;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Manages the SLF4J MDC (Mapped Diagnostic Context) lifecycle for every Camel exchange.
 *
 * <p>Call {@link #populateMDC(Exchange)} at route entry to stamp all subsequent log
 * lines with the correlation ID, exchange ID, and route ID. Call {@link #clearMDC()}
 * at route exit (or in a {@code doFinally} block) to prevent MDC leakage across
 * thread re-use in a thread-pooled environment.
 *
 * <h3>Sanitisation</h3>
 * <p>Inbound correlation IDs (read from HTTP headers) are sanitised before being
 * stored: ASCII control characters — including newlines and carriage returns — are
 * replaced with {@code _}, and the value is capped at
 * {@value #CORRELATION_ID_MAX_LEN} characters. This prevents log-injection attacks
 * where a crafted {@code X-Correlation-ID} header could forge log entries.
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

    public void clearMDC() {
        MDC.remove(LogConstants.MDC_CORRELATION_ID);
        MDC.remove(LogConstants.MDC_EXCHANGE_ID);
        MDC.remove(LogConstants.MDC_ROUTE_ID);
        MDC.remove(LogConstants.MDC_MESSAGE_TYPE);
        MDC.remove(LogConstants.MDC_SOURCE_SYSTEM);
        MDC.remove(LogConstants.MDC_TARGET_SYSTEM);
    }

    public void set(String key, String value) {
        if (value != null) {
            MDC.put(key, value);
        }
    }
}
