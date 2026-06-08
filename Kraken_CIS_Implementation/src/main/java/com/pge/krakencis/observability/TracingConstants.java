package com.pge.krakencis.observability;

import io.opentelemetry.api.common.AttributeKey;

/**
 * OpenTelemetry Integration — central registry of span names and span-attribute keys.
 *
 * <p>Pure constants holder for the Native OpenTelemetry SDK instrumentation. Contains
 * no business logic; nothing here changes application behaviour. Keeping span names and
 * attribute keys in one place guarantees the same identifiers are used everywhere a span
 * is created or enriched, so traces read consistently in Jaeger.
 */
public final class TracingConstants {

    private TracingConstants() {}

    /** Instrumentation (tracer) scope name — identifies spans produced by this app's SDK. */
    public static final String INSTRUMENTATION_NAME = "com.pge.krakencis.observability";

    // ── Processing-stage span names (see resources/otel/tracing-flow.md) ──────────
    public static final String SPAN_RECEIVED              = "RECEIVED";
    public static final String SPAN_VALIDATED             = "VALIDATED";
    public static final String SPAN_KAFKA_PUBLISH_START   = "KAFKA_PUBLISH_START";
    public static final String SPAN_KAFKA_PUBLISH_SUCCESS = "KAFKA_PUBLISH_SUCCESS";
    public static final String SPAN_KAFKA_CONSUME         = "KAFKA_CONSUME";
    public static final String SPAN_EXTERNAL_SERVICE_CALL = "EXTERNAL_SERVICE_CALL";
    public static final String SPAN_RESPONSE_SENT         = "RESPONSE_SENT";

    // ── Span attribute keys ───────────────────────────────────────────────────────
    public static final AttributeKey<String> ATTR_CORRELATION_ID     = AttributeKey.stringKey("correlation.id");
    public static final AttributeKey<String> ATTR_BUSINESS_IDENTIFIER = AttributeKey.stringKey("business.identifier");
    public static final AttributeKey<String> ATTR_ROUTE_ID           = AttributeKey.stringKey("route.id");
    public static final AttributeKey<String> ATTR_OPERATION          = AttributeKey.stringKey("operation");
    public static final AttributeKey<String> ATTR_SERVICE_NAME       = AttributeKey.stringKey("service.name");
    public static final AttributeKey<String> ATTR_KAFKA_TOPIC        = AttributeKey.stringKey("kafka.topic");

    // ── Exception attribute keys ──────────────────────────────────────────────────
    public static final AttributeKey<String> ATTR_EXCEPTION_TYPE     = AttributeKey.stringKey("exception.type");
    public static final AttributeKey<String> ATTR_EXCEPTION_MESSAGE  = AttributeKey.stringKey("exception.message");

    /** W3C trace-context header propagated across Kafka records and outbound HTTP calls. */
    public static final String TRACEPARENT_HEADER = "traceparent";
}
