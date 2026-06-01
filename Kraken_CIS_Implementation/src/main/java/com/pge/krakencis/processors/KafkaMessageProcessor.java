package com.pge.krakencis.processors;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.KrakenEvent;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.stereotype.Component;

/**
 * Prepares a single KrakenEvent for Kafka publishing by setting the
 * message key and any additional Kafka headers before marshalling.
 */
@Component
public class KafkaMessageProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(KafkaMessageProcessor.class);

    @Override
    public void process(Exchange exchange) throws Exception {
        KrakenEvent event       = exchange.getIn().getBody(KrakenEvent.class);
        String      correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID,
                                                          String.class);

        // Kafka message key — used for partition routing and idempotency
        exchange.getIn().setHeader(KafkaConstants.KEY, event.getMessageId());

        // Pass correlationId as a Kafka header for downstream traceability
        exchange.getIn().setHeader(LogConstants.PROP_CORRELATION_ID, correlationId);

        log.debug("kafkaMessagePrepared", correlationId,
            "messageId",  event.getMessageId(),
            "externalId", event.getExternalId());
    }
}
