package com.pge.krakencis.processors.rcdc.request;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.BaseProcessor;
import com.pge.krakencis.services.SOARCDCRequestService;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

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
