package com.pge.krakencis.processors.rcdc.request;

import com.pge.krakencis.exceptions.ExternalServiceException;
import com.pge.krakencis.exceptions.RetryableException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.BaseProcessor;
import com.pge.krakencis.services.NetworkExceptionUtils;
import com.pge.krakencis.services.SOARCDCRequestService;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

/**
 * Delegates the outbound HTTP call to the SOA-RCDC target service.
 *
 * <p>Network-level errors are wrapped in {@link RetryableException} so Camel's
 * redelivery policy retries them before routing to the retry queue.
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

        // OpenTelemetry Java Agent Migration — Native SDK removed.
        // The custom EXTERNAL_SERVICE_CALL span (formerly wrapping this call via TracingHelper /
        // ExceptionTracingHelper) is removed; the Java Agent's HTTP-client instrumentation creates
        // and exports the outbound-call span automatically. Control flow and error routing below
        // are unchanged. No business logic changes.
        try {
            int statusCode = soaRcdcRequestService.sendCommand(jsonPayload, correlationId);
            exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, statusCode);
            log.debug("rcdcTargetCallDelegated", correlationId, "httpStatus", statusCode);
        } catch (RetryableException e) {
            // HttpClientService already exhausted its in-process retries and raised a
            // RetryableException with no ExternalServiceException in the cause chain.
            // Re-throw as-is so Camel routes it to the retry topic.
            throw e;
        } catch (Exception e) {
            // Network-level failures (ConnectException, SocketTimeout, etc.)
            // Pass the root network cause only — NOT the ExternalServiceException wrapper —
            // so Camel's onException(ExternalServiceException) does not match the cause chain
            // and route this to DLQ instead of the retry topic.
            if (NetworkExceptionUtils.isNetworkException(e)) {
                Throwable networkCause = NetworkExceptionUtils.rootNetworkCause((Throwable) e);
                log.warn("rcdcTargetNetworkError", correlationId,
                    "error", e.getMessage(), "exceptionType", e.getClass().getSimpleName());
                throw RetryableException.networkError(
                    "Network error calling " + SERVICE_NAME, correlationId, networkCause);
            }
            throw e;
        }
    }
}
