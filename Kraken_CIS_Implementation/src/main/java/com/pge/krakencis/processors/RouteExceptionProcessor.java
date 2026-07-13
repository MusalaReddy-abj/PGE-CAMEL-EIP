package com.pge.krakencis.processors;

import com.pge.krakencis.exceptions.KrakenBaseException;
import com.pge.krakencis.exceptions.TransformationException;
import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.AuditLogger;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.models.ErrorResponse;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Factory for typed Camel {@link Processor} instances that convert domain exceptions
 * into structured {@link ErrorResponse} JSON bodies.
 *
 * <h3>HTTP status mapping</h3>
 * <ul>
 *   <li>{@link ValidationException}     → 400 — includes list of field violations</li>
 *   <li>{@link TransformationException} → 422 — mapping/serialization failure</li>
 *   <li>{@link KrakenBaseException}     → 500 — domain error</li>
 *   <li>{@link Exception}               → 503 after retries — includes retry count + cause</li>
 * </ul>
 *
 * <h3>Response body (all errors)</h3>
 * <pre>
 * {
 *   "status":        400,
 *   "errorCode":     "VAL-002",
 *   "message":       "Required field is missing: payload.events",
 *   "correlationId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
 *   "timestamp":     "2025-12-18T10:00:00+05:30",
 *   "detail":        [...]          // present only when applicable
 * }
 * </pre>
 */
@Component
public class RouteExceptionProcessor {

    private final AuditLogger auditLogger;

    public RouteExceptionProcessor(AuditLogger auditLogger) {
        this.auditLogger = auditLogger;
    }

    // ── Exception-type handlers ───────────────────────────────────────────────

    /** Handles {@link ValidationException} → HTTP 400. Detail: list of violation messages. */
    public Processor validation(String operation) {
        return exchange -> {
            ValidationException ex = exchange.getProperty(
                Exchange.EXCEPTION_CAUGHT, ValidationException.class);
            auditLogger.logFailure(exchange, operation, ex);
            respond(exchange, 400, ex.getErrorCode().getCode(), ex.getMessage(),
                ex.getViolations().isEmpty() ? null : ex.getViolations());
        };
    }

    /** Handles {@link TransformationException} → HTTP 422. */
    public Processor transformation(String operation) {
        return exchange -> {
            TransformationException ex = exchange.getProperty(
                Exchange.EXCEPTION_CAUGHT, TransformationException.class);
            auditLogger.logFailure(exchange, operation, ex);
            respond(exchange, 422, ex.getErrorCode().getCode(), ex.getMessage(), null);
        };
    }

    /** Handles {@link KrakenBaseException} → HTTP 500. */
    public Processor domain(String operation) {
        return exchange -> {
            KrakenBaseException ex = exchange.getProperty(
                Exchange.EXCEPTION_CAUGHT, KrakenBaseException.class);
            auditLogger.logFailure(exchange, operation, ex);
            respond(exchange, 500, ex.getErrorCode().getCode(), ex.getMessage(), null);
        };
    }

    /** Handles unexpected {@link Exception} → HTTP 500. */
    public Processor system(String operation) {
        return exchange -> {
            Exception ex = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
            auditLogger.logFailure(exchange, operation, ex);
            respond(exchange, 500, "SYS-001", "Internal server error", null);
        };
    }

    /**
     * Handles transient {@link Exception} after all 3 retry attempts are exhausted → HTTP 503.
     * Detail includes {@code retriesAttempted} count and the root-cause message.
     */
    public Processor systemWithRetryInfo(String operation) {
        return exchange -> {
            Exception ex  = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
            int retries   = exchange.getIn().getHeader(Exchange.REDELIVERY_COUNTER, 0, Integer.class);
            auditLogger.logFailure(exchange, operation, ex);

            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("retriesAttempted", retries);
            detail.put("cause", ex != null ? ex.getMessage() : "Unknown error");

            String code = "SYS-001";
            String message = "Service temporarily unavailable after " + retries + " retry attempt(s)";

            if (ex instanceof KrakenBaseException krakenEx) {
                code = krakenEx.getErrorCode().getCode();
                message = krakenEx.getMessage();
                detail.putAll(krakenEx.getContext());
            }

            if (operation != null && operation.startsWith("postOnDemandRead")) {
                respondSoapFault(exchange, 500, code, message, detail);
                return;
            }

            respond(exchange, 503, code, message, detail);
        };
    }

    // ── Core response builder ─────────────────────────────────────────────────

    private void respond(Exchange exchange, int httpStatus,
                          String code, String message, Object detail) {

        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);

        ErrorResponse errorResponse = ErrorResponse.builder()
            .status(httpStatus)
            .errorCode(code)
            .message(message)
            .correlationId(correlationId)
            .timestamp(OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
            .detail(detail)
            .build();

        // Set the ErrorResponse OBJECT (not a pre-serialized String). On JSON-binding routes
        // (/alarms, /rcdc/response) the REST binding marshals it to a JSON object; on
        // bindingMode=off routes (/rcdc, /odr) ErrorResponseTypeConverter converts it to a JSON
        // string for sending. Either way the client gets a proper JSON object. Setting a String
        // here made the JSON-binding routes DOUBLE-ENCODE it into a quoted, escaped string.
        exchange.getIn().setBody(errorResponse);
        exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, httpStatus);
        exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/json");
    }

    private void respondSoapFault(Exchange exchange, int httpStatus,
                                  String code, String message, Object detail) {

        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String faultMessage = message != null ? message : "OnDemandRead request failed";

        String body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\">\n"
            + "  <soapenv:Header/>\n"
            + "  <soapenv:Body>\n"
            + "    <soapenv:Fault>\n"
            + "      <faultcode>soapenv:Server</faultcode>\n"
            + "      <faultstring>" + escapeXml(faultMessage) + "</faultstring>\n"
            + "      <detail>\n"
            + "        <errorCode>" + escapeXml(code) + "</errorCode>\n"
            + "        <correlationId>" + escapeXml(correlationId) + "</correlationId>\n"
            + "        <timestamp>" + OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "</timestamp>\n"
            + "        <diagnostic>" + escapeXml(String.valueOf(detail)) + "</diagnostic>\n"
            + "      </detail>\n"
            + "    </soapenv:Fault>\n"
            + "  </soapenv:Body>\n"
            + "</soapenv:Envelope>";

        exchange.getIn().setBody(body);
        exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, httpStatus);
        exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "text/xml; charset=utf-8");
    }

    private String escapeXml(String value) {
        if (value == null) {
            return "";
        }
        return value
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\"", "&quot;")
            .replace("'", "&apos;");
    }
}
