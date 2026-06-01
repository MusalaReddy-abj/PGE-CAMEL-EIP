package com.pge.krakencis.processors.alarms;

import com.pge.krakencis.logging.AuditLogger;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.models.alarms.AlarmProcessingResponse;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AlarmResponseProcessor extends BaseProcessor {

    private final AuditLogger auditLogger;

    @Value("${kafka.topic.alarms}")
    private String alarmsTopic;

    public AlarmResponseProcessor(AuditLogger auditLogger) {
        this.auditLogger = auditLogger;
    }

    @Override
    public void process(Exchange exchange) {
        int    count         = exchange.getIn().getBody(Integer.class);
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        long   startTime     = exchange.getProperty(LogConstants.PROP_START_TIME, 0L, Long.class);

        auditLogger.logSuccess(exchange, "postAlarms", System.currentTimeMillis() - startTime);

        exchange.getIn().setBody(AlarmProcessingResponse.builder()
            .status("ACCEPTED")
            .correlationId(correlationId)
            .eventsPublished(count)
            .topic(alarmsTopic)
            .build());

        exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 202);
        exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/json");
        exchange.getIn().setHeader("X-Correlation-ID", correlationId);
    }
}
