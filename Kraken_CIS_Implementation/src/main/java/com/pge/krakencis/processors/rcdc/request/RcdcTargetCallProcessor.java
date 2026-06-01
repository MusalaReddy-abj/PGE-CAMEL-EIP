package com.pge.krakencis.processors.rcdc.request;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.BaseProcessor;
import com.pge.krakencis.services.SOARCDCRequestService;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

/**
 * Delegates the outbound HTTP call to the SOA-RCDC target service.
 *
 * <p>Reads the pre-serialised JSON payload from the exchange body, calls
 * {@link com.pge.krakencis.services.SOARCDCRequestService#sendCommand}, and stores
 * the returned HTTP status code as the {@code Exchange.HTTP_RESPONSE_CODE} header
 * for inspection by the downstream
 * {@link RcdcTargetResponseProcessor}.
 *
 * <p>This processor does not interpret the status code — it only delegates and
 * records it. Non-2xx handling is the sole responsibility of
 * {@link RcdcTargetResponseProcessor}.
 */
@Component
public class RcdcTargetCallProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(RcdcTargetCallProcessor.class);

    private final SOARCDCRequestService soaRcdcRequestService;

    public RcdcTargetCallProcessor(SOARCDCRequestService soaRcdcRequestService) {
        this.soaRcdcRequestService = soaRcdcRequestService;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String jsonPayload   = exchange.getIn().getBody(String.class);
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);

        int statusCode = soaRcdcRequestService.sendCommand(jsonPayload, correlationId);

        exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, statusCode);
        log.debug("rcdcTargetCallDelegated", correlationId, "httpStatus", statusCode);
    }
}
