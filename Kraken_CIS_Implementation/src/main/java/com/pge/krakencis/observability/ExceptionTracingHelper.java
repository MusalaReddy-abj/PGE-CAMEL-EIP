package com.pge.krakencis.observability;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;

/**
 * OpenTelemetry Integration — records exceptions on spans.
 *
 * <p>Marks a span failed ({@link StatusCode#ERROR}), attaches the exception event, and stamps
 * {@code exception.type} / {@code exception.message} attributes. The caller re-throws the
 * original exception unchanged, so error-handling behaviour is unaffected — this only annotates
 * the trace. Null-safe. No business logic changes.
 */
public final class ExceptionTracingHelper {

    private ExceptionTracingHelper() {}

    /** Records {@code throwable} on {@code span} and sets its status to ERROR. */
    public static void recordException(Span span, Throwable throwable) {
        if (span == null || !span.getSpanContext().isValid() || throwable == null) {
            return;
        }
        span.recordException(throwable);
        String message = throwable.getMessage() != null
                ? throwable.getMessage()
                : throwable.getClass().getSimpleName();
        span.setStatus(StatusCode.ERROR, message);
        span.setAttribute(TracingConstants.ATTR_EXCEPTION_TYPE, throwable.getClass().getName());
        if (throwable.getMessage() != null) {
            span.setAttribute(TracingConstants.ATTR_EXCEPTION_MESSAGE, throwable.getMessage());
        }
    }
}
