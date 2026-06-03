package com.pge.krakencis.processors.odr;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.BaseProcessor;
import com.pge.krakencis.services.SOAOdrMockService;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

@Component
public class OdrMockCallProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(OdrMockCallProcessor.class);

    private final SOAOdrMockService soaOdrMockService;

    public OdrMockCallProcessor(SOAOdrMockService soaOdrMockService) {
        this.soaOdrMockService = soaOdrMockService;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String rawXml        = exchange.getProperty("odr.rawXml", String.class);
        String mRID          = exchange.getProperty("odr.mRID", String.class);

        log.info("odrMockCallStarted", correlationId, "mRID", mRID);

        String responseXml = soaOdrMockService.sendRequest(rawXml, correlationId);

        exchange.getIn().setBody(responseXml);
        exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 200);
        exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "text/xml; charset=utf-8");
        exchange.getIn().setHeader("X-Correlation-ID", correlationId);

        log.info("odrMockCallCompleted", correlationId, "mRID", mRID);
    }
}
