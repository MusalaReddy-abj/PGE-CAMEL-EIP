package com.pge.krakencis.processors;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class CorrelationIdProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(CorrelationIdProcessor.class);

    @Override
    public void process(Exchange exchange) {
        String id = exchange.getIn().getHeader("X-Correlation-ID", String.class);
        boolean generated = (id == null || id.isBlank());
        if (generated) {
            id = UUID.randomUUID().toString();
        }
        exchange.setProperty(LogConstants.PROP_CORRELATION_ID, id);
        exchange.getIn().setHeader("X-Correlation-ID", id);

        log.debug("correlationId", id, "source", generated ? "generated" : "header");
    }
}
