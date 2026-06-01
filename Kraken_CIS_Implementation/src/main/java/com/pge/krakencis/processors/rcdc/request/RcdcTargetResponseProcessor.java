package com.pge.krakencis.processors.rcdc.request;

import com.pge.krakencis.exceptions.ExternalServiceException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

/**
 * Inspects the HTTP status code set by {@link RcdcTargetCallProcessor} and raises
 * a typed exception for any non-2xx outcome.
 *
 * <p>On success (HTTP 2xx) the exchange passes through unchanged.
 * On failure the processor throws
 * {@link com.pge.krakencis.exceptions.ExternalServiceException} so that the
 * enclosing route's {@code doCatch} block can log it and the {@code doFinally}
 * block can commit the Kafka offset.
 *
 * <p>If {@code Exchange.HTTP_RESPONSE_CODE} is absent (defaults to {@code 0}) the
 * result is also treated as a failure, preventing silent pass-through when the
 * upstream call processor threw before setting the header.
 */
@Component
public class RcdcTargetResponseProcessor extends BaseProcessor {

    private static final StructuredLogger log         = StructuredLogger.of(RcdcTargetResponseProcessor.class);
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
        throw ExternalServiceException.httpError(SERVICE_NAME, status, responseBody, correlationId);
    }
}
