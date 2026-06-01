package com.pge.krakencis.processors.rcdc.response;

import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.rcdc.response.RcdcHesResponseMessage;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

@Component
public class RcdcHesResponseProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(RcdcHesResponseProcessor.class);

    @Override
    public void process(Exchange exchange) throws Exception {
        RcdcHesResponseMessage response = exchange.getIn().getBody(RcdcHesResponseMessage.class);

        validate(response);

        String correlationId = response.getHeader().getCorrelationID();
        exchange.setProperty(LogConstants.PROP_CORRELATION_ID, correlationId);
        exchange.getIn().setHeader("X-Correlation-ID", correlationId);
        exchange.setProperty(LogConstants.KAFKA_KEY, correlationId);

        log.info("rcdcHesResponseReceived", correlationId,
            "verb",      response.getHeader().getVerb(),
            "replyCode", response.getReply().getReplyCode(),
            "mRID",      response.getPayload().getDefaultResponse().getEndDeviceAsset().getMRID());
    }

    private void validate(RcdcHesResponseMessage r) {
        if (r == null)                            throw ValidationException.missingField("ResponseMessage", null);
        if (r.getHeader() == null)                throw ValidationException.missingField("Header", null);
        if (r.getHeader().getCorrelationID() == null) throw ValidationException.missingField("Header.CorrelationID", null);
        String cid = r.getHeader().getCorrelationID();
        if (r.getReply() == null || r.getReply().getReplyCode() == null)
            throw ValidationException.missingField("Reply.ReplyCode", cid);
        if (r.getPayload() == null || r.getPayload().getDefaultResponse() == null
                || r.getPayload().getDefaultResponse().getEndDeviceAsset() == null
                || r.getPayload().getDefaultResponse().getEndDeviceAsset().getMRID() == null)
            throw ValidationException.missingField("Payload.DefaultResponse.EndDeviceAsset.mRID", cid);
    }
}
