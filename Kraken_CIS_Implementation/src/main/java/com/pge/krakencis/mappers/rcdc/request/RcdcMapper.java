package com.pge.krakencis.mappers.rcdc.request;

import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.rcdc.request.RcdcRequest;
import com.pge.krakencis.models.rcdc.request.target.RcdcTargetEndDeviceAsset;
import com.pge.krakencis.models.rcdc.request.target.RcdcTargetHeader;
import com.pge.krakencis.models.rcdc.request.target.RcdcTargetPayload;
import com.pge.krakencis.models.rcdc.request.target.RcdcTargetRequest;
import com.pge.krakencis.models.rcdc.request.target.RcdcTargetSwitchState;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class RcdcMapper {

    private static final StructuredLogger   log       = StructuredLogger.of(RcdcMapper.class);
    private static final DateTimeFormatter  TIMESTAMP = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    private static final String             SOURCE    = "KRAKEN-CIS";

    public RcdcTargetRequest toTargetRequest(RcdcRequest request, String correlationId) {
        String mRID  = request.getPayload().getRcdSwitchState().getEndDeviceAsset().getMRID();
        String state = request.getPayload().getRcdSwitchState().getState().trim().toUpperCase();

        RcdcTargetRequest target = RcdcTargetRequest.builder()
            .header(RcdcTargetHeader.builder()
                .verb(request.getHeader().getVerb())
                .noun(request.getHeader().getNoun())
                .timestamp(OffsetDateTime.now().format(TIMESTAMP))
                .source(SOURCE)
                .replyAddress(request.getHeader().getReplyAddress())
                .correlationID(correlationId)
                .build())
            .payload(RcdcTargetPayload.builder()
                .rcdSwitchState(RcdcTargetSwitchState.builder()
                    .endDeviceAsset(RcdcTargetEndDeviceAsset.builder().mRID(mRID).build())
                    .state(state)
                    .build())
                .build())
            .build();

        log.info("rcdcTargetRequestMapped", correlationId, "mRID", mRID, "state", state);
        return target;
    }
}
