package com.pge.krakencis.processors.rcdc.request;

import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.rcdc.request.RcdcRequest;
import com.pge.krakencis.models.rcdc.request.RcdcState;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

/**
 * Validates an inbound {@link com.pge.krakencis.models.rcdc.request.RcdcRequest}
 * and seeds the exchange with the values that downstream processors depend on.
 *
 * <p>Validation is performed eagerly: every required field is checked before any
 * exchange property is written. A missing or invalid field throws a
 * {@link com.pge.krakencis.exceptions.ValidationException}, which the enclosing
 * route's exception handler maps to an HTTP 400 response.
 *
 * <p>Exchange properties written by this processor:
 * <ul>
 *   <li>{@code X-Correlation-ID} — from {@code header.correlationID}</li>
 *   <li>{@code rcdc.mRID} — meter identifier from the switch-state payload</li>
 *   <li>{@code rcdc.state} — resolved {@link com.pge.krakencis.models.rcdc.request.RcdcState}
 *       enum name ({@code CONNECT} or {@code DISCONNECT})</li>
 * </ul>
 */
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
            ValidationException ex = ValidationException
                .invalidFormat("state", "CONNECT | DISCONNECT", correlationId);
            ex.withContext("providedValue", state);
            throw ex;
        }
    }
}
