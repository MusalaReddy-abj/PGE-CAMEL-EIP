package com.pge.krakencis.logging;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

/**
 * Pair these processors at the start and end of a Camel route:
 *
 *   from("direct:myRoute")
 *       .process(routeLoggingProcessor.entry("myOperation"))
 *       ...
 *       .process(routeLoggingProcessor.exit("myOperation"));
 */
@Component
public class RouteLoggingProcessor {

    private static final StructuredLogger log = StructuredLogger.of(RouteLoggingProcessor.class);

    private final MDCContextManager mdcContextManager;
    private final AuditLogger auditLogger;

    public RouteLoggingProcessor(MDCContextManager mdcContextManager, AuditLogger auditLogger) {
        this.mdcContextManager = mdcContextManager;
        this.auditLogger       = auditLogger;
    }

    public Processor entry(String operation) {
        return exchange -> {
            mdcContextManager.populateMDC(exchange);
            exchange.setProperty(LogConstants.PROP_START_TIME, System.currentTimeMillis());
            auditLogger.logRouteStart(exchange, operation);

            String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID,
                                                         String.class);
            log.debug("routeEntry", correlationId,
                "operation", operation,
                "bodyType", bodyType(exchange));
        };
    }

    public Processor exit(String operation) {
        return exchange -> {
            Long startTime = exchange.getProperty(LogConstants.PROP_START_TIME, Long.class);
            long duration  = startTime != null ? System.currentTimeMillis() - startTime : -1;

            auditLogger.logRouteEnd(exchange, operation, duration);
            mdcContextManager.clearMDC();
        };
    }

    private String bodyType(Exchange exchange) {
        Object body = exchange.getIn().getBody();
        return body != null ? body.getClass().getSimpleName() : "null";
    }
}
