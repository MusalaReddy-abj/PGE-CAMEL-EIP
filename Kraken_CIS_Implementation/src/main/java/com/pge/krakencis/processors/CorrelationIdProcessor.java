package com.pge.krakencis.processors;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Seeds the {@code X-Correlation-ID} exchange property and header at the start of
 * every HTTP-inbound route.
 *
 * <p>Resolution order:
 * <ol>
 *   <li>Read the {@code X-Correlation-ID} HTTP request header.</li>
 *   <li>If absent or blank, generate a random UUID.</li>
 * </ol>
 * The sanitised ID is stored in both the exchange property
 * ({@link com.pge.krakencis.logging.LogConstants#PROP_CORRELATION_ID}) and the
 * {@code X-Correlation-ID} response header so callers can correlate their own logs.
 *
 * <p>For Kafka consumer routes, correlation IDs are extracted from the Kafka message
 * key instead — see the inline processor in each Kafka listener.
 */
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
