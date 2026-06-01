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
 * Factory for typed Camel {@link org.apache.camel.Processor} instances that convert
 * domain exceptions into structured JSON error responses.
 *
 * <p>Intended to be used exclusively through {@link com.pge.krakencis.routes.BaseRoute}.
 * Each factory method returns a {@code Processor} that:
 * <ol>
 *   <li>Extracts the exception from {@code Exchange.EXCEPTION_CAUGHT}.</li>
 *   <li>Logs the failure via {@link com.pge.krakencis.logging.AuditLogger}.</li>
 *   <li>Sets an appropriate HTTP status code and a JSON body containing
 *       {@code errorCode} and {@code message}.</li>
 * </ol>
 *
 * <h3>HTTP status mapping</h3>
 * <ul>
 *   <li>{@link com.pge.krakencis.exceptions.ValidationException} → 400</li>
 *   <li>{@link com.pge.krakencis.exceptions.TransformationException} → 422</li>
 *   <li>{@link com.pge.krakencis.exceptions.KrakenBaseException} → 500</li>
 *   <li>{@link Exception} → 500 with code {@code SYS-001}</li>
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
