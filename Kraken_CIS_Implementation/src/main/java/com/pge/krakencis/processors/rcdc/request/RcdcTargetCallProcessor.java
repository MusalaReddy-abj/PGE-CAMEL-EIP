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

        try {
            int statusCode = soaRcdcRequestService.sendCommand(jsonPayload, correlationId);
            exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, statusCode);
            log.debug("rcdcTargetCallDelegated", correlationId, "httpStatus", statusCode);
        } catch (Exception e) {
            // Check full cause chain — Spring's RestClient wraps ConnectException inside
            // ResourceAccessException, which HttpClientService wraps in ExternalServiceException.
            // NetworkExceptionUtils now traverses the entire chain.
            if (NetworkExceptionUtils.isNetworkException(e)) {
                log.warn("rcdcTargetNetworkError", correlationId,
                    "error", e.getMessage(), "exceptionType", e.getClass().getSimpleName());
                throw RetryableException.networkError(
                    "Network error calling " + SERVICE_NAME, correlationId, e);
            }
            // Safety net: ExternalServiceException with no HTTP status = connection-level failure
            // (e.g. HttpClientService exhausted its internal retries on a refused connection)
            if (e instanceof ExternalServiceException ese && ese.getHttpStatusCode() == null) {
                log.warn("rcdcTargetConnectionFailure", correlationId,
                    "error", e.getMessage());
                throw RetryableException.networkError(
                    "Connection failure calling " + SERVICE_NAME, correlationId, e);
            }
            throw e;
        }
    }
}
