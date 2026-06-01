package com.pge.krakencis.logging;

import org.apache.camel.Exchange;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class MDCContextManager {

    public String initCorrelationId(Exchange exchange) {
        String correlationId = exchange.getIn().getHeader(
            LogConstants.PROP_CORRELATION_ID, String.class);

        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        exchange.setProperty(LogConstants.PROP_CORRELATION_ID, correlationId);
        exchange.getIn().setHeader(LogConstants.PROP_CORRELATION_ID, correlationId);
        return correlationId;
    }

    public void populateMDC(Exchange exchange) {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID,
                                                     String.class);
        if (correlationId == null) {
            correlationId = initCorrelationId(exchange);
        }

        MDC.put(LogConstants.MDC_CORRELATION_ID, correlationId);
        MDC.put(LogConstants.MDC_EXCHANGE_ID,    exchange.getExchangeId());

        if (exchange.getFromRouteId() != null) {
            MDC.put(LogConstants.MDC_ROUTE_ID, exchange.getFromRouteId());
        }
    }

    public void clearMDC() {
        MDC.remove(LogConstants.MDC_CORRELATION_ID);
        MDC.remove(LogConstants.MDC_EXCHANGE_ID);
        MDC.remove(LogConstants.MDC_ROUTE_ID);
        MDC.remove(LogConstants.MDC_MESSAGE_TYPE);
        MDC.remove(LogConstants.MDC_SOURCE_SYSTEM);
        MDC.remove(LogConstants.MDC_TARGET_SYSTEM);
    }

    public void set(String key, String value) {
        if (value != null) {
            MDC.put(key, value);
        }
    }
}
