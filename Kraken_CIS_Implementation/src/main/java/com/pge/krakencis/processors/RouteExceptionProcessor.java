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
 * Reusable exception processor for any Camel route.
 *
 * Usage in any RouteBuilder:
 *   .onException(ValidationException.class)
 *       .handled(true).process(exceptionProcessor.validation("myOperation")).end()
 *   .onException(TransformationException.class)
 *       .handled(true).process(exceptionProcessor.transformation("myOperation")).end()
 *   .onException(KrakenBaseException.class)
 *       .handled(true).process(exceptionProcessor.domain("myOperation")).end()
 *   .onException(Exception.class)
 *       .handled(true).process(exceptionProcessor.system("myOperation")).end()
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
