package com.pge.krakencis.logging;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Pair these processors at the start and end of every Camel route to stamp
 * structured audit records and populate MDC for all log lines within the route.
 *
 * <h3>MDC fields populated on entry</h3>
 * <ul>
 *   <li>{@code correlationId} — from exchange property</li>
 *   <li>{@code routeId}       — from Camel exchange</li>
 *   <li>{@code exchangeId}    — Camel internal ID</li>
 *   <li>{@code sourceSystem}  — inferred from operation name (see {@link #SYSTEM_MAP})</li>
 *   <li>{@code targetSystem}  — inferred from operation name</li>
 * </ul>
 *
 * All MDC fields are cleared on exit to prevent leakage in a thread-pooled JVM.
 */
@Component
public class RouteLoggingProcessor {

    private static final StructuredLogger log = StructuredLogger.of(RouteLoggingProcessor.class);

    /**
     * Maps operation name → [sourceSystem, targetSystem].
     * Used to populate {@link LogConstants#MDC_SOURCE_SYSTEM} and
     * {@link LogConstants#MDC_TARGET_SYSTEM} without requiring each route to pass them explicitly.
     */
    private static final Map<String, String[]> SYSTEM_MAP = Map.of(
        "postAlarms",             new String[]{"HES",          "KAFKA"},
        "postRCDC",               new String[]{"HES",          "KAFKA"},
        "postRCDCHESResponse",    new String[]{"HES",          "KAFKA"},
        "consumeRcdcRequest",     new String[]{"KAFKA",        "MOCK-RCDC"},
        "consumeRcdcHesResponse", new String[]{"KAFKA",        "MOCK-MDM"},
        "retryRcdcRequest",       new String[]{"KAFKA-RETRY",  "MOCK-RCDC"},
        "retryRcdcHesResponse",   new String[]{"KAFKA-RETRY",  "MOCK-MDM"},
        "profileReadsFtpPoll",    new String[]{"FTP",          "KAFKA"},
        "profileReadsS3Poll",     new String[]{"S3",           "KAFKA"}
    );

    private final MDCContextManager mdcContextManager;
    private final AuditLogger       auditLogger;

    public RouteLoggingProcessor(MDCContextManager mdcContextManager, AuditLogger auditLogger) {
        this.mdcContextManager = mdcContextManager;
        this.auditLogger       = auditLogger;
    }

    /** Standard entry — infers sourceSystem/targetSystem from operation name. */
    public Processor entry(String operation) {
        String[] systems = SYSTEM_MAP.getOrDefault(operation, new String[]{"KRAKEN-CIS", "DOWNSTREAM"});
        return entry(operation, systems[0], systems[1]);
    }

    /** Explicit entry — caller provides sourceSystem and targetSystem directly. */
    public Processor entry(String operation, String sourceSystem, String targetSystem) {
        return exchange -> {
            mdcContextManager.populateMDC(exchange);
            mdcContextManager.set(LogConstants.MDC_SOURCE_SYSTEM, sourceSystem);
            mdcContextManager.set(LogConstants.MDC_TARGET_SYSTEM, targetSystem);

            exchange.setProperty(LogConstants.PROP_START_TIME, System.currentTimeMillis());
            auditLogger.logRouteStart(exchange, operation);

            String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
            log.debug("routeEntry", correlationId,
                "operation",    operation,
                "sourceSystem", sourceSystem,
                "targetSystem", targetSystem,
                "bodyType",     bodyType(exchange));
        };
    }

    public Processor exit(String operation) {
        return exchange -> {
            Long startTime = exchange.getProperty(LogConstants.PROP_START_TIME, Long.class);
            long duration  = startTime != null ? System.currentTimeMillis() - startTime : -1;
            // Audit log runs first so it still carries traceId/spanId in MDC
            auditLogger.logRouteEnd(exchange, operation, duration);
            // Close OTel scope → span ends → OTel bridge removes traceId/spanId from MDC
            mdcContextManager.endTrace(exchange);
            mdcContextManager.clearMDC();
        };
    }

    private String bodyType(Exchange exchange) {
        Object body = exchange.getIn().getBody();
        return body != null ? body.getClass().getSimpleName() : "null";
    }
}
