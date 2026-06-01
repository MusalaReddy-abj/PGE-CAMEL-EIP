package com.pge.krakencis.processors.rcdc.response;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class RcdcHesAckProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(RcdcHesAckProcessor.class);

    @Value("${kafka.topic.rcdc-hes-response:kraken-rcdc-hes-response-events}")
    private String rcdcHesResponseTopic;

    @Override
    public void process(Exchange exchange) {
        int    published     = exchange.getIn().getBody(Integer.class);
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);

        Map<String, Object> ack = new LinkedHashMap<>();
        ack.put("status",          "ACCEPTED");
        ack.put("correlationId",   correlationId);
        ack.put("eventsPublished", published);
        ack.put("topic",           rcdcHesResponseTopic);

        exchange.getIn().setBody(ack);
        exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 202);
        exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/json");
        exchange.getIn().setHeader("X-Correlation-ID", correlationId);

        log.info("rcdcHesAckSent", correlationId, "eventsPublished", published);
    }
}
