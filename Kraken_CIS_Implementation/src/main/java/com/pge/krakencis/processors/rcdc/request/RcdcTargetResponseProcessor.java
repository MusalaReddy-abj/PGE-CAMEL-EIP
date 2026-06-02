package com.pge.krakencis.processors.rcdc.request;

import com.pge.krakencis.exceptions.ExternalServiceException;
import com.pge.krakencis.exceptions.RetryableException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

/**
 * Inspects the HTTP status code set by {@link RcdcTargetCallProcessor} and raises
 * a typed exception for any non-2xx outcome.
 *
 * <ul>
 *   <li>2xx  → success, passes through unchanged</li>
 *   <li>5xx / 408 / 429 → {@link RetryableException} (Camel retries up to 3×)</li>
 *   <li>4xx (other) → {@link ExternalServiceException} (permanent, routed to DLQ)</li>
 *   <li>0 (no status set) → treated as connection failure → {@link RetryableException}</li>
 * </ul>
 */
@Component
public class RcdcTargetResponseProcessor extends BaseProcessor {

    private static final StructuredLogger log          = StructuredLogger.of(RcdcTargetResponseProcessor.class);
    private static final String           SERVICE_NAME = "SOA-RCDC-TargetService";

    @Override
    public void process(Exchange exchange) throws Exception {
        int    status        = exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, 0, Integer.class);
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);

        if (status >= 200 && status < 300) {
            log.info("rcdcTargetCallSucceeded", correlationId, "httpStatus", status);
            return;
        }

        String responseBody = exchange.getIn().getBody(String.class);
        log.warn("rcdcTargetCallFailed", correlationId,
            "httpStatus", status, "responseBody", responseBody);

        // 5xx, 408 (timeout), 429 (rate-limit), 0 (connection failure) → retryable
        if (status == 0 || status >= 500 || status == 408 || status == 429) {
            throw RetryableException.transient_(
                SERVICE_NAME + " returned HTTP " + status, correlationId);
        }
        // 4xx client errors → permanent failure, goes to DLQ
        throw ExternalServiceException.httpError(SERVICE_NAME, status, responseBody, correlationId);
    }
}
