package com.pge.krakencis.logging;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Scope;
import org.apache.camel.Exchange;
import org.apache.camel.spi.Synchronization;

/**
 * Starts a fresh root span for <b>poll-driven</b> Camel routes (S3, FTP, timer).
 *
 * <p><b>Why this is needed.</b> The OpenTelemetry Java Agent starts an entry-point span
 * for HTTP (servlet/Undertow) and Kafka (kafka-clients) consumers, but <b>not</b> for
 * polling consumers such as {@code aws2-s3}, {@code ftp}, or {@code timer}. Without a
 * route span, the only telemetry produced is disconnected AWS-SDK / Kafka-producer client
 * spans with nothing tying them into a single trace, and {@code Span.current()} is invalid
 * at route entry (so {@link RouteLoggingProcessor} cannot stamp {@code correlation_id}).
 *
 * <p>This starts a {@code CONSUMER} root span, makes it current for the rest of the route
 * (so all downstream client spans nest under it into one trace), and ends it when the
 * exchange completes via a {@link Synchronization} on the same routing thread. Unlike
 * {@link KafkaTraceContext}, there is no upstream context to adopt — a polled file/tick is
 * the start of a new trace. Fully guarded: a tracing failure never breaks processing.
 */
public final class RouteRootSpan {

    private static final StructuredLogger log = StructuredLogger.of(RouteRootSpan.class);
    private static final String INSTRUMENTATION_SCOPE = "com.pge.krakencis.routes";

    private RouteRootSpan() {}

    /**
     * Starts a CONSUMER root span named {@code spanName} and makes it current for the
     * remainder of the route. No-op if the unit of work is unavailable (the span must be
     * ended on the same thread that opened the scope).
     */
    public static void start(Exchange exchange, String spanName) {
        if (exchange == null || exchange.getUnitOfWork() == null) {
            return;
        }
        try {
            Span span = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_SCOPE)
                    .spanBuilder(spanName)
                    // Force a brand-new trace. Poll consumers run on long-lived, reused threads
                    // (S3/FTP poll, timer) where the Agent may leave stale context; without
                    // setNoParent() this span would attach to that older trace ("appended to
                    // older traces") instead of starting its own.
                    .setNoParent()
                    .setSpanKind(SpanKind.CONSUMER)
                    .startSpan();
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
            // Tracing must never break message/file processing.
            log.debug("routeRootSpanFailed", null, "span", spanName, "error", e.getMessage());
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
