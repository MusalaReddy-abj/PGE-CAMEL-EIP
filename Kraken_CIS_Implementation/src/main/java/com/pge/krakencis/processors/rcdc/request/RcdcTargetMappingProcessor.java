package com.pge.krakencis.processors.rcdc.request;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.mappers.rcdc.request.RcdcMapper;
import com.pge.krakencis.models.rcdc.request.RcdcRequest;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

@Component
public class RcdcTargetMappingProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(RcdcTargetMappingProcessor.class);

    private final RcdcMapper rcdcMapper;

    public RcdcTargetMappingProcessor(RcdcMapper rcdcMapper) {
        this.rcdcMapper = rcdcMapper;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        RcdcRequest request       = exchange.getIn().getBody(RcdcRequest.class);
        String      correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);

        exchange.getIn().setBody(rcdcMapper.toTargetRequest(request, correlationId));
        exchange.setProperty(LogConstants.KAFKA_KEY, correlationId);

        log.debug("rcdcTargetMapped", correlationId,
            "mRID",  request.getPayload().getRcdSwitchState().getEndDeviceAsset().getMRID());
    }
}
