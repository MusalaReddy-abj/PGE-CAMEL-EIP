package com.pge.krakencis.exceptions.handler;

import com.pge.krakencis.exceptions.KrakenBaseException;
import com.pge.krakencis.exceptions.RetryableException;
import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;

/**
 * @deprecated Superseded by per-route handlers in BaseRoute.processingRoute() and
 *             by doTry/doCatch blocks in Kafka consumer routes. Keeping the source
 *             for reference only — remove this file when all routes use BaseRoute.
 *
 *             @Component is intentionally absent so Spring does not load this bean.
 */
public class CamelExceptionHandler extends RouteBuilder {

    private static final StructuredLogger log = StructuredLogger.of(CamelExceptionHandler.class);

    @Override
    public void configure() {

        // Validation — do not retry, return 400-equivalent header
        onException(ValidationException.class)
            .handled(true)
            .maximumRedeliveries(0)
            .process(this::handleKrakenException)
            .setHeader("CamelHttpResponseCode", constant(400));

        // Retryable transient errors — retry with backoff
        onException(RetryableException.class)
            .handled(true)
            .maximumRedeliveries(3)
            .redeliveryDelay(2000)
            .backOffMultiplier(2)
            .useExponentialBackOff()
            .process(this::handleKrakenException);

        // All other domain exceptions — no retry, log and propagate metadata
        onException(KrakenBaseException.class)
            .handled(true)
            .maximumRedeliveries(0)
            .process(this::handleKrakenException);

        // Unexpected exceptions — single retry then dead-letter
        onException(Exception.class)
            .handled(true)
            .maximumRedeliveries(1)
            .redeliveryDelay(1000)
            .process(this::handleUnexpectedException);
    }

    private void handleKrakenException(Exchange exchange) {
        KrakenBaseException ex = exchange.getProperty(Exchange.EXCEPTION_CAUGHT,
                                                       KrakenBaseException.class);
        if (ex == null) return;

        log.error("camelExceptionCaught", ex.getCorrelationId(),
            LogConstants.FIELD_ERROR_CODE, ex.getErrorCode().getCode(),
            LogConstants.FIELD_ERROR_CATEGORY, ex.getErrorCategory().name(),
            "message", ex.getMessage());

        exchange.setProperty("errorCode", ex.getErrorCode().getCode());
        exchange.setProperty("errorCategory", ex.getErrorCategory().name());
        exchange.setProperty("errorMessage", ex.getMessage());
        exchange.setProperty("errorContext", ex.getContext());
    }

    private void handleUnexpectedException(Exchange exchange) {
        Exception ex = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID,
                                                     String.class);
        if (ex == null) return;

        log.error("unexpectedExceptionCaught", correlationId,
            "exceptionType", ex.getClass().getName(),
            "message", ex.getMessage());

        exchange.setProperty("errorCode", "SYS-001");
        exchange.setProperty("errorCategory", "SYSTEM");
        exchange.setProperty("errorMessage", "An unexpected error occurred");
    }
}
