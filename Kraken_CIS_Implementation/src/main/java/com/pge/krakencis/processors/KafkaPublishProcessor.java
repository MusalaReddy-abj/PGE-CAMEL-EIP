package com.pge.krakencis.processors;

import com.pge.krakencis.logging.AuditLogger;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.stereotype.Component;

/**
 * Provides two processors for the Kafka publish flow:
 *
 *   prepare() — sets the Kafka message key and logs the outbound event
 *   audit()   — logs the successful Kafka publish after toD() completes
 */
@Component
public class KafkaPublishProcessor {

    private static final StructuredLogger log = StructuredLogger.of(KafkaPublishProcessor.class);

    private final AuditLogger auditLogger;

    public KafkaPublishProcessor(AuditLogger auditLogger) {
        this.auditLogger = auditLogger;
    }

    /** Sets KafkaConstants.KEY from KAFKA_KEY property and logs the send attempt. */
    public Processor prepare() {
        return exchange -> {
            String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID,
                                                         String.class);
            String topic = exchange.getProperty(LogConstants.KAFKA_TOPIC, String.class);
            String key   = exchange.getProperty(LogConstants.KAFKA_KEY,   String.class);

            if (key != null) {
                exchange.getIn().setHeader(KafkaConstants.KEY, key);
            }

            log.debug("kafkaSending", correlationId,
                "topic", topic,
                "key",   key != null ? key : "none");
        };
    }

    /** Logs a successful publish after the message has been sent to Kafka. */
    public Processor audit() {
        return exchange -> {
            String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID,
                                                         String.class);
            String topic = exchange.getProperty(LogConstants.KAFKA_TOPIC, String.class);
            String key   = exchange.getIn().getHeader(KafkaConstants.KEY, String.class);

            auditLogger.logKafkaPublish(correlationId, topic, key, true);
        };
    }
}
