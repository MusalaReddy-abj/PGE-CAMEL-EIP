package com.pge.krakencis.logging;

import com.pge.krakencis.exceptions.KrakenBaseException;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

@Component
public class AuditLogger {

    private static final StructuredLogger log = StructuredLogger.of("AUDIT");

    public void logRouteStart(Exchange exchange, String operation) {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        log.info("routeStart", correlationId,
            LogConstants.FIELD_STATUS,    LogConstants.STATUS_STARTED,
            "operation",                  operation,
            LogConstants.MDC_ROUTE_ID,    exchange.getFromRouteId(),
            LogConstants.MDC_EXCHANGE_ID, exchange.getExchangeId());
    }

    public void logRouteEnd(Exchange exchange, String operation, long durationMs) {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        log.info("routeEnd", correlationId,
            LogConstants.FIELD_STATUS,      LogConstants.STATUS_COMPLETED,
            "operation",                    operation,
            LogConstants.FIELD_DURATION_MS, durationMs,
            LogConstants.MDC_ROUTE_ID,      exchange.getFromRouteId());
    }

    public void logSuccess(Exchange exchange, String operation, long durationMs) {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        log.info("operationSuccess", correlationId,
            LogConstants.FIELD_STATUS,      LogConstants.STATUS_SUCCESS,
            "operation",                    operation,
            LogConstants.FIELD_DURATION_MS, durationMs);
    }

    public void logFailure(Exchange exchange, String operation, KrakenBaseException ex) {
        String correlationId = ex.getCorrelationId() != null
            ? ex.getCorrelationId()
            : exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        log.error("operationFailure", correlationId, ex,
            LogConstants.FIELD_STATUS,        LogConstants.STATUS_FAILURE,
            "operation",                      operation,
            LogConstants.FIELD_ERROR_CODE,    ex.getErrorCode().getCode(),
            LogConstants.FIELD_ERROR_CATEGORY, ex.getErrorCategory().name(),
            "message",                        ex.getMessage());
    }

    public void logFailure(Exchange exchange, String operation, Exception ex) {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        log.error("operationFailure", correlationId, ex,
            LogConstants.FIELD_STATUS,     LogConstants.STATUS_FAILURE,
            "operation",                   operation,
            LogConstants.FIELD_ERROR_CODE, "SYS-001",
            "exceptionType",               ex.getClass().getSimpleName(),
            "message",                     ex.getMessage());
    }

    public void logKafkaPublish(String correlationId, String topic,
                                 String messageId, boolean success) {
        log.info("kafkaPublish", correlationId,
            LogConstants.MDC_TARGET_SYSTEM,  "kafka",
            "topic",                         topic,
            "messageId",                     messageId,
            LogConstants.FIELD_STATUS,       success ? LogConstants.STATUS_SUCCESS
                                                     : LogConstants.STATUS_FAILURE);
    }

    public void logExternalCall(String serviceName, String operation,
                                 String correlationId, long durationMs, boolean success) {
        log.info("externalCall", correlationId,
            LogConstants.MDC_TARGET_SYSTEM,  serviceName,
            "operation",                     operation,
            LogConstants.FIELD_STATUS,       success ? LogConstants.STATUS_SUCCESS
                                                     : LogConstants.STATUS_FAILURE,
            LogConstants.FIELD_DURATION_MS,  durationMs);
    }
}
