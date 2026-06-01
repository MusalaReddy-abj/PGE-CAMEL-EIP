package com.pge.krakencis.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Structured logging wrapper. Usage:
 *   private static final StructuredLogger log = StructuredLogger.of(MyClass.class);
 *   log.info("orderProcessed", correlationId, "orderId", id, "amount", 99.99);
 */
public final class StructuredLogger {

    private final Logger logger;

    private StructuredLogger(Class<?> clazz) {
        this.logger = LoggerFactory.getLogger(clazz);
    }

    private StructuredLogger(String name) {
        this.logger = LoggerFactory.getLogger(name);
    }

    public static StructuredLogger of(Class<?> clazz) {
        return new StructuredLogger(clazz);
    }

    public static StructuredLogger of(String name) {
        return new StructuredLogger(name);
    }

    // ── info ──────────────────────────────────────────────────────────────────

    public void info(String event, String correlationId, Object... kvPairs) {
        if (!logger.isInfoEnabled()) return;
        withMDC(event, correlationId, kvPairs, () ->
            logger.info(buildMessage(event, kvPairs)));
    }

    // ── debug ─────────────────────────────────────────────────────────────────

    public void debug(String event, String correlationId, Object... kvPairs) {
        if (!logger.isDebugEnabled()) return;
        withMDC(event, correlationId, kvPairs, () ->
            logger.debug(buildMessage(event, kvPairs)));
    }

    // ── warn ──────────────────────────────────────────────────────────────────

    public void warn(String event, String correlationId, Object... kvPairs) {
        if (!logger.isWarnEnabled()) return;
        withMDC(event, correlationId, kvPairs, () ->
            logger.warn(buildMessage(event, kvPairs)));
    }

    // ── error ─────────────────────────────────────────────────────────────────

    public void error(String event, String correlationId, Object... kvPairs) {
        withMDC(event, correlationId, kvPairs, () ->
            logger.error(buildMessage(event, kvPairs)));
    }

    public void error(String event, String correlationId, Throwable throwable,
                      Object... kvPairs) {
        withMDC(event, correlationId, kvPairs, () ->
            logger.error(buildMessage(event, kvPairs), throwable));
    }

    // ── internal ──────────────────────────────────────────────────────────────

    private void withMDC(String event, String correlationId,
                          Object[] kvPairs, Runnable logAction) {
        Map<String, String> added = new LinkedHashMap<>();
        try {
            putIfAbsent(added, LogConstants.FIELD_EVENT, event);
            putIfAbsent(added, LogConstants.MDC_CORRELATION_ID, correlationId);

            for (int i = 0; i + 1 < kvPairs.length; i += 2) {
                String key = String.valueOf(kvPairs[i]);
                String val = kvPairs[i + 1] != null ? String.valueOf(kvPairs[i + 1]) : "null";
                putIfAbsent(added, key, val);
            }

            logAction.run();
        } finally {
            added.keySet().forEach(MDC::remove);
        }
    }

    private void putIfAbsent(Map<String, String> added, String key, String value) {
        if (key != null && value != null && MDC.get(key) == null) {
            MDC.put(key, value);
            added.put(key, value);
        }
    }

    private String buildMessage(String event, Object[] kvPairs) {
        if (kvPairs == null || kvPairs.length == 0) return event;

        StringBuilder sb = new StringBuilder(event);
        for (int i = 0; i + 1 < kvPairs.length; i += 2) {
            sb.append(' ').append(kvPairs[i]).append('=').append(kvPairs[i + 1]);
        }
        return sb.toString();
    }
}
