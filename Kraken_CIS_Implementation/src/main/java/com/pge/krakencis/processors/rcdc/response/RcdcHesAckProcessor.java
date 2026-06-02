package com.pge.krakencis.processors.rcdc.response;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.alarms.AlarmProcessingResponse;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Builds the HTTP 202 acknowledgement returned to HES after its response message
 * is accepted and queued to Kafka.
 *
 * <p>Uses {@link AlarmProcessingResponse} — the same response model as the Alarm
 * endpoint — since both carry identical fields: status, correlationId,
 * eventsPublished, and topic.
 */
@Component
public class RcdcHesAckProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(RcdcHesAckProcessor.class);

    @Value("${kafka.topic.rcdc-hes-response:kraken-rcdc-hes-response-events}")
    private String rcdcHesResponseTopic;

    @Override
    public void process(Exchange exchange) {
        int    published     = exchange.getIn().getBody(Integer.class);
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);

        exchange.getIn().setBody(AlarmProcessingResponse.builder()
            .status("ACCEPTED")
            .correlationId(correlationId)
            .eventsPublished(published)
            .topic(rcdcHesResponseTopic)
            .build());

        exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 202);
        exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/json");
        exchange.getIn().setHeader("X-Correlation-ID", correlationId);

        log.info("rcdcHesAckSent", correlationId, "eventsPublished", published);
    }
}
