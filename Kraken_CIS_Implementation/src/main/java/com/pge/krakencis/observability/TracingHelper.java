package com.pge.krakencis.observability;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

/**
 * OpenTelemetry Integration — thin, null-safe façade for creating custom spans.
 *
 * <p>Static accessor style mirrors the existing {@link com.pge.krakencis.logging.SpanEnricher}
 * so it can be dropped into Camel route DSL without changing any route constructor. Every
 * method is a no-op until {@link #init(OpenTelemetry)} has run and is fully null-safe, so it
 * is always safe to call from business code without guarding. No business logic changes.
 */
public final class TracingHelper {

    private TracingHelper() {}

    private static volatile OpenTelemetry openTelemetry;
    private static volatile Tracer tracer;

    /** Wired once by {@link OpenTelemetryConfig} at startup. */
    static void init(OpenTelemetry otel) {
        openTelemetry = otel;
        tracer = otel.getTracer(TracingConstants.INSTRUMENTATION_NAME);
    }

    /** @return the SDK instance, or {@code null} if tracing is disabled / not yet initialised. */
    static OpenTelemetry openTelemetry() {
        return openTelemetry;
    }

    public static boolean isReady() {
        return tracer != null;
    }

    // ── Point-in-time stage markers (children of the current Camel span) ───────────

    /**
     * Records a short-lived processing-stage span (e.g. {@code RECEIVED}, {@code VALIDATED})
     * as a child of the currently-active span, stamps the standard attributes and ends it
     * immediately. Produces sibling stage markers under the inbound REST/Kafka span.
     */
    public static void recordStage(Exchange exchange, String spanName) {
        if (tracer == null) {
            return;
        }
        Span span = tracer.spanBuilder(spanName).setSpanKind(SpanKind.INTERNAL).startSpan();
        try {
            SpanAttributeHelper.stamp(span, exchange);
        } finally {
            span.end();
        }
    }

    /** Camel {@link Processor} factory so stage markers slot straight into route DSL. */
    public static Processor stage(String spanName) {
        return exchange -> recordStage(exchange, spanName);
    }

    /**
     * Records a stage marker parented to the trace context extracted from the message headers
     * (used on Kafka consume so the consumer span joins the producer's trace).
     */
    public static void recordStageWithExtractedParent(Exchange exchange, String spanName) {
        if (tracer == null) {
            return;
        }
        Context parent = ContextPropagationHelper.extractFromHeaders(exchange);
        Span span = tracer.spanBuilder(spanName)
                .setParent(parent)
                .setSpanKind(SpanKind.CONSUMER)
                .startSpan();
        try {
            SpanAttributeHelper.stamp(span, exchange);
        } finally {
            span.end();
        }
    }

    // ── Wrapping span for external calls ──────────────────────────────────────────

    /**
     * Starts a CLIENT span (e.g. {@code EXTERNAL_SERVICE_CALL}) parented to the context
     * extracted from the message headers, makes it current, and returns a {@link SpanInScope}
     * the caller must {@link SpanInScope#close() close} in a {@code finally} block. While the
     * span is current, downstream instrumented HTTP clients propagate its {@code traceparent}.
     */
    public static SpanInScope startExternalCall(Exchange exchange, String spanName) {
        if (tracer == null) {
            return SpanInScope.NOOP;
        }
        Context parent = ContextPropagationHelper.extractFromHeaders(exchange);
        Span span = tracer.spanBuilder(spanName)
                .setParent(parent)
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        SpanAttributeHelper.stamp(span, exchange);
        Scope scope = span.makeCurrent();
        return new SpanInScope(span, scope);
    }

    /**
     * Holds an active span and its scope. {@link #close()} closes the scope then ends the span;
     * it is idempotent and always safe to call (including the {@link #NOOP} instance).
     */
    public static final class SpanInScope implements AutoCloseable {

        static final SpanInScope NOOP = new SpanInScope(null, null);

        private final Span span;
        private final Scope scope;

        SpanInScope(Span span, Scope scope) {
            this.span = span;
            this.scope = scope;
        }

        /** The active span, or an invalid no-op span — never {@code null}. */
        public Span span() {
            return span != null ? span : Span.getInvalid();
        }

        @Override
        public void close() {
            if (scope != null) {
                scope.close();
            }
            if (span != null) {
                span.end();
            }
        }
    }
}
