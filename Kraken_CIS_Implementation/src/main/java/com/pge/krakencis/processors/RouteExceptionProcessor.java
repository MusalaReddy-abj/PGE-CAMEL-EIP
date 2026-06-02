package com.pge.krakencis.processors;

import com.pge.krakencis.exceptions.KrakenBaseException;
import com.pge.krakencis.exceptions.TransformationException;
import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.AuditLogger;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Factory for typed Camel {@link Processor} instances that convert domain exceptions
 * into structured JSON error responses.
 *
 * <h3>HTTP status mapping</h3>
 * <ul>
 *   <li>{@link ValidationException}     → 400</li>
 *   <li>{@link TransformationException} → 422</li>
 *   <li>{@link KrakenBaseException}     → 500</li>
 *   <li>{@link Exception}               → 503 after retries (includes retry count)</li>
 * </ul>
 */
@Component
public class RouteExceptionProcessor {

    private final AuditLogger auditLogger;

    public RouteExceptionProcessor(AuditLogger auditLogger) {
        this.auditLogger = auditLogger;
    }

    public Processor validation(String operation) {
        return exchange -> {
            ValidationException ex = exchange.getProperty(
                Exchange.EXCEPTION_CAUGHT, ValidationException.class);
            auditLogger.logFailure(exchange, operation, ex);
            respond(exchange, 400, ex.getErrorCode().getCode(), ex.getMessage(),
                ex.getViolations().isEmpty() ? null : ex.getViolations());
        };
    }

    public Processor transformation(String operation) {
        return exchange -> {
            TransformationException ex = exchange.getProperty(
                Exchange.EXCEPTION_CAUGHT, TransformationException.class);
            auditLogger.logFailure(exchange, operation, ex);
            respond(exchange, 422, ex.getErrorCode().getCode(), ex.getMessage(), null);
        };
    }

    public Processor domain(String operation) {
        return exchange -> {
            KrakenBaseException ex = exchange.getProperty(
                Exchange.EXCEPTION_CAUGHT, KrakenBaseException.class);
            auditLogger.logFailure(exchange, operation, ex);
            respond(exchange, 500, ex.getErrorCode().getCode(), ex.getMessage(), null);
        };
    }

    public Processor system(String operation) {
        return exchange -> {
            Exception ex = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
            auditLogger.logFailure(exchange, operation, ex);
            respond(exchange, 500, "SYS-001", "Internal server error", null);
        };
    }

    /**
     * Used by HTTP listener routes after all 3 retry attempts are exhausted.
     * Returns HTTP 503 with the number of retries attempted so callers can surface it.
     */
    public Processor systemWithRetryInfo(String operation) {
        return exchange -> {
            Exception ex    = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
            int retries     = exchange.getIn().getHeader(Exchange.REDELIVERY_COUNTER, 0, Integer.class);
            auditLogger.logFailure(exchange, operation, ex);

            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("retriesAttempted", retries);
            detail.put("cause", ex != null ? ex.getMessage() : "Unknown error");

            respond(exchange, 503, "SYS-001",
                "Service temporarily unavailable after " + retries + " retry attempt(s)",
                detail);
        };
    }

    private void respond(Exchange exchange, int httpStatus,
                          String code, String message, Object detail) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("errorCode", code);
        body.put("message",   message);
        if (detail != null) body.put("detail", detail);

        exchange.getIn().setBody(body);
        exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, httpStatus);
        exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/json");
    }
}
