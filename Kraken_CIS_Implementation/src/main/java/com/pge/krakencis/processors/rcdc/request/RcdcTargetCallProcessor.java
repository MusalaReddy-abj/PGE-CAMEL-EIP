package com.pge.krakencis.processors.rcdc.request;

import com.pge.krakencis.exceptions.RetryableException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.BaseProcessor;
import com.pge.krakencis.services.SOARCDCRequestService;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

/**
 * Delegates the outbound HTTP call to the SOA-RCDC target service.
 *
 * <p>Network-level errors (connection refused, timeout, unknown host) are wrapped in
 * {@link RetryableException} so Camel's redelivery policy retries them up to 3 times
 * before routing to the retry queue.
 */
@Component
public class RcdcTargetCallProcessor extends BaseProcessor {

    private static final StructuredLogger log          = StructuredLogger.of(RcdcTargetCallProcessor.class);
    private static final String           SERVICE_NAME = "SOA-RCDC-TargetService";

    private final SOARCDCRequestService soaRcdcRequestService;

    public RcdcTargetCallProcessor(SOARCDCRequestService soaRcdcRequestService) {
        this.soaRcdcRequestService = soaRcdcRequestService;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String jsonPayload   = exchange.getIn().getBody(String.class);
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);

        try {
            int statusCode = soaRcdcRequestService.sendCommand(jsonPayload, correlationId);
            exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, statusCode);
            log.debug("rcdcTargetCallDelegated", correlationId, "httpStatus", statusCode);
        } catch (Exception e) {
            if (isNetworkException(e)) {
                log.warn("rcdcTargetNetworkError", correlationId,
                    "error", e.getMessage(), "exceptionType", e.getClass().getSimpleName());
                throw RetryableException.networkError(
                    "Network error calling " + SERVICE_NAME, correlationId, e);
            }
            throw e;
        }
    }

    private boolean isNetworkException(Exception e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        return cause instanceof ConnectException
            || cause instanceof SocketTimeoutException
            || cause instanceof UnknownHostException
            || cause instanceof InterruptedIOException
            || (e.getMessage() != null && (
                e.getMessage().contains("Connection refused")
             || e.getMessage().contains("timeout")
             || e.getMessage().contains("Temporary failure")));
    }
}
