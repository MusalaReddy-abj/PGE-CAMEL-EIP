package com.pge.krakencis.processors.odr;

import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.odr.request.OdrRequest;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

@Component
public class OdrRequestProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(OdrRequestProcessor.class);

    @Override
    public void process(Exchange exchange) throws Exception {
        OdrRequest request = exchange.getIn().getBody(OdrRequest.class);

        validate(request);

        String correlationId = request.getHeader().getCorrelationID();
        String mRID          = request.getPayload().getMeterReadings().getEndDeviceAsset().getMRID();

        exchange.setProperty(LogConstants.PROP_CORRELATION_ID, correlationId);
        exchange.getIn().setHeader("X-Correlation-ID", correlationId);
        exchange.setProperty("odr.mRID", mRID);

        log.info("odrRequestReceived", correlationId,
            "verb", request.getHeader().getVerb(),
            "noun", request.getHeader().getNoun(),
            "mRID", mRID);
    }

    private void validate(OdrRequest request) {
        if (request == null)
            throw ValidationException.missingField("requestMessage", null);
        if (request.getHeader() == null)
            throw ValidationException.missingField("header", null);
        if (request.getHeader().getCorrelationID() == null)
            throw ValidationException.missingField("header.correlationID", null);

        String cid = request.getHeader().getCorrelationID();

        if (request.getPayload() == null || request.getPayload().getMeterReadings() == null)
            throw ValidationException.missingField("payLoad.meterReadings", cid);
        if (request.getPayload().getMeterReadings().getEndDeviceAsset() == null
                || request.getPayload().getMeterReadings().getEndDeviceAsset().getMRID() == null)
            throw ValidationException.missingField("endDeviceAsset.mRID", cid);
    }
}
