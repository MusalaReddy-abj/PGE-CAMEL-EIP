package com.pge.krakencis.processors.rcdc.request;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

@Component
public class RcdcTargetResponseProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(RcdcTargetResponseProcessor.class);

    @Override
    public void process(Exchange exchange) throws Exception {
        int     status        = exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE, 0, Integer.class);
        String  correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        boolean success       = status >= 200 && status < 300;

        if (success) log.info("rcdcTargetCallSucceeded", correlationId, "httpStatus", status);
        else         log.warn("rcdcTargetCallFailed",    correlationId,
                         "httpStatus", status, "responseBody", exchange.getIn().getBody(String.class));
    }
}
