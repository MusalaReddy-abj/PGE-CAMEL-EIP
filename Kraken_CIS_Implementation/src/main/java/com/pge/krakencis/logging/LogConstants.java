package com.pge.krakencis.logging;

public final class LogConstants {

    // MDC keys
    public static final String MDC_CORRELATION_ID   = "correlationId";
    public static final String MDC_ROUTE_ID         = "routeId";
    public static final String MDC_EXCHANGE_ID      = "exchangeId";
    public static final String MDC_MESSAGE_TYPE     = "messageType";
    public static final String MDC_SOURCE_SYSTEM    = "sourceSystem";
    public static final String MDC_TARGET_SYSTEM    = "targetSystem";
    public static final String MDC_USER_ID          = "userId";

    // Structured log field keys
    public static final String FIELD_EVENT          = "event";
    public static final String FIELD_DURATION_MS    = "durationMs";
    public static final String FIELD_STATUS         = "status";
    public static final String FIELD_ERROR_CODE     = "errorCode";
    public static final String FIELD_ERROR_CATEGORY = "errorCategory";
    public static final String FIELD_COMPONENT      = "component";

    // Status values
    public static final String STATUS_SUCCESS       = "SUCCESS";
    public static final String STATUS_FAILURE       = "FAILURE";
    public static final String STATUS_STARTED       = "STARTED";
    public static final String STATUS_COMPLETED     = "COMPLETED";

    // Exchange property keys (stored on Camel exchange)
    public static final String PROP_START_TIME      = "routeStartTime";
    public static final String PROP_CORRELATION_ID  = "X-Correlation-ID";

    // Kafka exchange property keys — set by caller before to("direct:publishToKafka")
    public static final String KAFKA_TOPIC          = "kafkaTopic";
    public static final String KAFKA_KEY            = "kafkaKey";

    // Retry / error-routing exchange properties (Kafka consumer routes)
    public static final String PROP_ORIGINAL_BODY   = "originalBody";
    public static final String PROP_RETRY_TOPIC     = "retryTopic";
    public static final String PROP_DLQ_TOPIC       = "dlqTopic";
    public static final String PROP_SERVICE_NAME    = "serviceName";
    public static final String PROP_RETRY_ATTEMPT   = "retryAttempt";

    // Profile Reads CSV partial-failure exchange property
    // Value type: List<com.pge.krakencis.models.profilereads.ProfileReadFailedRow>
    public static final String PROP_FAILED_ROWS     = "profileReads.failedRows";

    // Profile Reads CSV processing counts — set by ProfileReadsCsvProcessor
    public static final String PROP_TOTAL_ROWS      = "profileReads.totalRows";
    public static final String PROP_SUCCESS_ROWS    = "profileReads.successRows";

    // Profile Reads file-level status — set by the route on success/partial/corrupted
    public static final String PROP_FILE_STATUS        = "profileReads.fileStatus";
    public static final String PROP_FILE_ERROR_MESSAGE = "profileReads.fileErrorMessage";

    // Profile Reads file status values
    public static final String FILE_STATUS_SUCCESS          = "SUCCESS";
    public static final String FILE_STATUS_PARTIAL_FAILURE  = "PARTIAL_FAILURE";
    public static final String FILE_STATUS_CORRUPTED        = "CORRUPTED";

    // OpenTelemetry Java Agent Migration: MDC keys written by the Java Agent's
    // logback-mdc instrumentation when a span is active (was traceId/spanId from
    // the removed Micrometer Tracing bridge). The PROP_OTEL_SPAN/SCOPE exchange
    // properties were removed — span lifecycle is now owned by the Java Agent.
    public static final String MDC_TRACE_ID         = "trace_id";
    public static final String MDC_SPAN_ID          = "span_id";

    private LogConstants() {}
}
