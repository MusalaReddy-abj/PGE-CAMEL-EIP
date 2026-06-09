package com.pge.krakencis.logging;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.spi.Synchronization;

import java.nio.charset.StandardCharsets;

/**
 * Re-joins a Camel Kafka consumer exchange to the producer's distributed trace.
 *
 * <p><b>Why this is needed.</b> The OpenTelemetry Java Agent injects the W3C
 * {@code traceparent} into the Kafka record headers on publish (below Camel, at
 * {@code KafkaProducer.send}), so the producer span is part of the inbound trace.
 * On the consume side, however, Camel iterates poll results via
 * {@code ConsumerRecords.records(partition)} — a path the Agent's kafka-clients
 * instrumentation does not wrap for context activation — so the consumer route
 * starts a brand-new root trace and everything after the publish appears
 * disconnected. (Note {@code traceparent} arrives as a {@code byte[]} header, the
 * same wire form {@link com.pge.krakencis.routes.BaseKafkaConsumerRoute} already
 * decodes for {@code X-Error-Attempt}.)
 *
 * <p><b>What it does.</b> Reads the propagated context off the record headers and
 * starts a {@code CONSUMER} span parented to it, made current for the remainder of
 * the route so every downstream span (HTTP calls, nested processors) joins the
 * original trace. The span is ended via a {@link Synchronization} when the exchange
 * completes — on the same (synchronous) consumer thread that opened the scope.
 *
 * <p><b>Safety.</b> Every operation is guarded; a tracing failure is logged at
 * debug and never propagates, so message processing is never affected. It is a
 * no-op when the record carries no valid trace context.
 */
public final class KafkaTraceContext {

    private static final StructuredLogger log = StructuredLogger.of(KafkaTraceContext.class);

    /** Instrumentation scope name for the manually-created consume span. */
    private static final String INSTRUMENTATION_SCOPE = "com.pge.krakencis.kafka";

    private KafkaTraceContext() {}

    /** Reads header values from the Camel IN message, decoding {@code byte[]} (Kafka wire form). */
    private static final TextMapGetter<Message> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Message carrier) {
            return carrier.getHeaders().keySet();
        }

        @Override
        public String get(Message carrier, String key) {
            if (carrier == null) {
                return null;
            }
            Object value = carrier.getHeader(key);
            if (value == null) {
                return null;
            }
            if (value instanceof byte[] bytes) {
                return new String(bytes, StandardCharsets.UTF_8);
            }
            return value.toString();
        }
    };

    /**
     * Extracts the producer trace context from the consumed record and starts a
     * CONSUMER span under it, made current for the rest of the route. No-op when no
     * valid remote context is present or the unit of work is unavailable.
     */
    public static void adopt(Exchange exchange) {
        if (exchange == null) {
            return;
        }
        try {
            // Scope cleanup must run on this same consumer thread; without a unit of
            // work we cannot guarantee that, so skip rather than leak an open scope.
            if (exchange.getUnitOfWork() == null) {
                log.debug("kafkaTraceAdoptSkipped", null, "reason", "no unit of work");
                return;
            }

            Context remote = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                    .extract(Context.root(), exchange.getIn(), GETTER);
            boolean hasRemote = Span.fromContext(remote).getSpanContext().isValid();

            String topic = exchange.getIn().getHeader(KafkaConstants.TOPIC, String.class);
            SpanBuilder builder = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_SCOPE)
                    .spanBuilder("process " + (topic != null ? topic : "kafka"))
                    .setSpanKind(SpanKind.CONSUMER);
            if (hasRemote) {
                // Join the producer's distributed trace.
                builder.setParent(remote);
            } else {
                // No upstream trace context on the record → start a fresh root. Crucially do
                // NOT fall through to the ambient Context.current(): consumer threads are
                // reused, so a leaked span would make this consume append to an older trace.
                builder.setNoParent();
            }
            Span span = builder.startSpan();
            if (topic != null) {
                span.setAttribute("messaging.system", "kafka");
                span.setAttribute("messaging.operation", "process");
                span.setAttribute("messaging.destination.name", topic);
            }

            Scope scope = span.makeCurrent();
            exchange.getUnitOfWork().addSynchronization(new Synchronization() {
                @Override
                public void onComplete(Exchange ex) {
                    end(span, scope, null);
                }

                @Override
                public void onFailure(Exchange ex) {
                    end(span, scope, ex.getException());
                }
            });
        } catch (Exception e) {
            // Tracing must never break message processing.
            log.debug("kafkaTraceAdoptFailed", null, "error", e.getMessage());
        }
    }

    private static void end(Span span, Scope scope, Throwable error) {
        try {
            if (error != null) {
                span.recordException(error);
            }
        } finally {
            scope.close();
            span.end();
        }
    }
}
