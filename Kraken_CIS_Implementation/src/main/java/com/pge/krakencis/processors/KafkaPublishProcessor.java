package com.pge.krakencis.processors;

import com.pge.krakencis.logging.AuditLogger;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.beans.factory.ObjectProvider;
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

    static final String METRIC_KAFKA_PUBLISH_ATTEMPTED = "kafka.publish.attempted";
    static final String METRIC_KAFKA_PUBLISH_SUCCESS   = "kafka.publish.success";

    private final AuditLogger    auditLogger;
    private final MeterRegistry  meterRegistry;
    private final OpenTelemetry  openTelemetry;

    public KafkaPublishProcessor(AuditLogger auditLogger,
                                 MeterRegistry meterRegistry,
                                 ObjectProvider<OpenTelemetry> openTelemetryProvider) {
        this.auditLogger   = auditLogger;
        this.meterRegistry = meterRegistry;
        // Use the Spring-managed OpenTelemetry SDK; fall back to no-op if tracing is disabled.
        this.openTelemetry = openTelemetryProvider.getIfAvailable(OpenTelemetry::noop);
    }

    /**
     * Sets the Kafka message key and propagates the correlation ID as a Kafka record
     * header so downstream consumers can read it without parsing the message key.
     *
     * <p>Headers set on every published record:
     * <ul>
     *   <li>{@code CamelKafkaKey} (message key) — from {@code KAFKA_KEY} property,
     *       or falls back to the correlation ID when not explicitly set.</li>
     *   <li>{@code X-Correlation-ID} (record header) — always the correlation ID,
     *       enabling trace-level correlation independent of the key.</li>
     * </ul>
     */
    public Processor prepare() {
        return exchange -> {
            String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID,
                                                         String.class);
            String topic = exchange.getProperty(LogConstants.KAFKA_TOPIC, String.class);
            String key   = exchange.getProperty(LogConstants.KAFKA_KEY,   String.class);

            // Use explicit key if set, otherwise fall back to correlationId so every
            // message is keyed — guarantees ordered delivery per correlation ID.
            String effectiveKey = (key != null) ? key : correlationId;
            if (effectiveKey != null) {
                exchange.getIn().setHeader(KafkaConstants.KEY, effectiveKey);
            }

            // Propagate correlation ID as a Kafka record header for end-to-end tracing.
            // Downstream consumers can read this header without parsing the message body.
            if (correlationId != null) {
                exchange.getIn().setHeader("X-Correlation-ID", correlationId);
            }

            // Inject the W3C trace context (traceparent / tracestate) into the outgoing
            // Kafka record headers. camel-opentelemetry on the consumer side extracts it
            // when it starts the consumer span, so the consume span becomes a CHILD of
            // this producer's span — one trace tree spanning the Kafka boundary.
            openTelemetry.getPropagators().getTextMapPropagator().inject(
                Context.current(),
                exchange.getIn(),
                (message, headerKey, value) -> {
                    if (message != null && value != null) {
                        message.setHeader(headerKey, value);
                    }
                });

            if (topic != null) {
                meterRegistry.counter(METRIC_KAFKA_PUBLISH_ATTEMPTED, "topic", topic).increment();
            }
            log.debug("kafkaSending", correlationId,
                "topic", topic,
                "key",   effectiveKey != null ? effectiveKey : "none");
        };
    }

    /** Logs a successful publish after the message has been sent to Kafka. */
    public Processor audit() {
        return exchange -> {
            String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID,
                                                         String.class);
            String topic = exchange.getProperty(LogConstants.KAFKA_TOPIC, String.class);
            String key   = exchange.getIn().getHeader(KafkaConstants.KEY, String.class);

            if (topic != null) {
                meterRegistry.counter(METRIC_KAFKA_PUBLISH_SUCCESS, "topic", topic).increment();
            }
            auditLogger.logKafkaPublish(correlationId, topic, key, true);
        };
    }
}
