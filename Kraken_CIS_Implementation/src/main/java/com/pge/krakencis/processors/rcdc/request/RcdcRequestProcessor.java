package com.pge.krakencis.processors.rcdc.request;

import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.rcdc.request.RcdcRequest;
import com.pge.krakencis.models.rcdc.request.RcdcState;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

@Component
public class RcdcRequestProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(RcdcRequestProcessor.class);

    @Override
    public void process(Exchange exchange) throws Exception {
        RcdcRequest request = exchange.getIn().getBody(RcdcRequest.class);

        validate(request);

        String correlationId = request.getHeader().getCorrelationID();
        String mRID          = request.getPayload().getRcdSwitchState().getEndDeviceAsset().getMRID();
        String state         = request.getPayload().getRcdSwitchState().getState();

        exchange.setProperty(LogConstants.PROP_CORRELATION_ID, correlationId);
        exchange.getIn().setHeader("X-Correlation-ID", correlationId);

        RcdcState rcdcState = resolveState(state, correlationId);

        log.info("rcdcRequestReceived", correlationId,
            "verb", request.getHeader().getVerb(), "mRID", mRID, "state", rcdcState.name());

        exchange.setProperty("rcdc.mRID",  mRID);
        exchange.setProperty("rcdc.state", rcdcState.name());
    }

    private void validate(RcdcRequest request) {
        if (request == null)
            throw ValidationException.missingField("requestMessage", null);
        if (request.getHeader() == null)
            throw ValidationException.missingField("header", null);
        if (request.getHeader().getCorrelationID() == null)
            throw ValidationException.missingField("header.correlationID", null);
        String cid = request.getHeader().getCorrelationID();
        if (request.getPayload() == null || request.getPayload().getRcdSwitchState() == null)
            throw ValidationException.missingField("payLoad.RCDSwitchState", cid);
        if (request.getPayload().getRcdSwitchState().getEndDeviceAsset() == null
                || request.getPayload().getRcdSwitchState().getEndDeviceAsset().getMRID() == null)
            throw ValidationException.missingField("endDeviceAsset.mRID", cid);
        if (request.getPayload().getRcdSwitchState().getState() == null)
            throw ValidationException.missingField("state", cid);
    }

    private RcdcState resolveState(String state, String correlationId) {
        try {
            return RcdcState.from(state);
        } catch (IllegalArgumentException e) {
            throw (ValidationException) ValidationException
                .invalidFormat("state", "CONNECT | DISCONNECT", correlationId)
                .withContext("providedValue", state);
        }
    }
}
