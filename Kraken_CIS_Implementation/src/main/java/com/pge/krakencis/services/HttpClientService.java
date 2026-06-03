package com.pge.krakencis.services;

import com.pge.krakencis.configs.HttpClientProperties;
import com.pge.krakencis.exceptions.ErrorCode;
import com.pge.krakencis.exceptions.ExternalServiceException;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.security.JwtTokenProvider;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClient.RequestBodySpec;

import java.net.URI;
import java.util.List;
import java.util.Objects;

/**
 * Common HTTP sender for all outbound calls (REST JSON and SOAP XML).
 *
 * <h3>Retry strategy</h3>
 * <p>Transient failures — connection errors and HTTP status codes in
 * {@code http.client.retry.retryable-status-codes} (default: 429, 500, 502, 503, 504) —
 * are retried up to {@code http.client.retry.max-attempts} times using exponential
 * back-off ({@code initialDelayMs × backoffMultiplier^n}, capped at {@code maxDelayMs}).
 * Individual service endpoints can override the attempt count via
 * {@link HttpOutboundRequest#getMaxRetryAttempts()}.
 *
 * <h3>Failure routing</h3>
 * <pre>
 *   send()
 *     ├─ attempt 1..N  (in-process, exponential back-off)
 *     │    ├─ HTTP 2xx           → return success
 *     │    ├─ retryable error    → wait &amp; retry
 *     │    └─ non-retryable 4xx  → publish to DLQ → throw
 *     └─ retries exhausted       → publish to retry-queue → throw
 * </pre>
 *
 * <h3>Kafka topics</h3>
 * <ul>
 *   <li>{@code http.client.retry-topic} — transient failures after in-process retries</li>
 *   <li>{@code http.client.dlq-topic}   — permanent failures (non-retryable or manual)</li>
 * </ul>
 */
@Service
public class HttpClientService {

    private static final StructuredLogger log = StructuredLogger.of(HttpClientService.class);

    private final RestClient           restClient;
    private final HttpClientProperties httpClientProperties;
    private final DlqPublisher         dlqPublisher;
    private final JwtTokenProvider     jwtTokenProvider;

    /**
     * @param restClientBuilder Spring Boot auto-configures this with:
     *   - {@code ObservationRestClientCustomizer} → injects Micrometer Tracing observations
     *     so every outbound call gets a child span and propagates {@code traceparent} /
     *     {@code b3} headers to downstream services automatically.
     *   - {@code MicrometerHttpClientObservationConvention} → records HTTP client metrics.
     */
    public HttpClientService(RestClient.Builder    restClientBuilder,
                              HttpClientProperties  httpClientProperties,
                              DlqPublisher          dlqPublisher,
                              JwtTokenProvider      jwtTokenProvider) {
        this.httpClientProperties = httpClientProperties;
        this.jwtTokenProvider     = jwtTokenProvider;
        this.dlqPublisher         = dlqPublisher;
        this.restClient           = restClientBuilder.build();
    }

    /**
     * Sends an HTTP request with automatic retry and Kafka failure routing.
     *
     * @param request       protocol-agnostic request descriptor
     * @param correlationId end-to-end trace ID propagated to all log lines and headers
     * @return successful response (always HTTP 2xx)
     * @throws ExternalServiceException after all retries are exhausted or for non-retryable errors
     */
    public HttpOutboundResponse send(HttpOutboundRequest request, String correlationId) {
        Assert.notNull(request, "HttpOutboundRequest must not be null");
        Assert.hasText(request.getUrl(), "request.url must not be empty");

        int  maxAttempts = resolveMaxAttempts(request);
        long delayMs     = httpClientProperties.getRetry().getInitialDelayMs();

        ExternalServiceException lastError = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return doSend(request, correlationId, attempt, maxAttempts);

            } catch (ExternalServiceException e) {
                lastError = e;

                if (!isRetryable(e.getHttpStatusCode())) {
                    log.error("httpOutboundNonRetryable", correlationId, e,
                        "service",     request.getServiceName(),
                        "url",         request.getUrl(),
                        "httpStatus",  e.getHttpStatusCode(),
                        "attempt",     attempt);
                    dlqPublisher.publishToDlq(request, e, correlationId, attempt, maxAttempts);
                    throw e;
                }

                if (attempt < maxAttempts) {
                    log.warn("httpOutboundRetrying", correlationId,
                        "service",      request.getServiceName(),
                        "url",          request.getUrl(),
                        "httpStatus",   e.getHttpStatusCode(),
                        "attempt",      attempt,
                        "maxAttempts",  maxAttempts,
                        "nextDelayMs",  delayMs);
                    sleep(delayMs, correlationId);
                    delayMs = nextDelay(delayMs);
                }
            }
        }

        if (lastError == null) {
            // Defensive: should not be reachable when maxAttempts >= 1
            throw ExternalServiceException.unavailable(request.getUrl(), correlationId, null);
        }
        log.error("httpOutboundRetriesExhausted", correlationId, lastError,
            "service",     request.getServiceName(),
            "url",         request.getUrl(),
            "maxAttempts", maxAttempts);
        dlqPublisher.publishToRetryQueue(request, lastError, correlationId, maxAttempts, maxAttempts);
        throw lastError;
    }

    // ── private ───────────────────────────────────────────────────────────────

    private HttpOutboundResponse doSend(HttpOutboundRequest request, String correlationId,
                                         int attempt, int maxAttempts) {
        long startTime = System.currentTimeMillis();

        log.debug("httpOutboundAttempt", correlationId,
            "service",     request.getServiceName(),
            "method",      request.getMethod(),
            "url",         request.getUrl(),
            "contentType", request.getContentType(),
            "attempt",     attempt,
            "maxAttempts", maxAttempts);

        try {
            String url         = Objects.requireNonNull(request.getUrl(),         "url");
            String method      = Objects.requireNonNull(request.getMethod(),      "method");
            String contentType = Objects.requireNonNull(request.getContentType(), "contentType");
            String body        = request.getBody() != null ? request.getBody() : "";
            String corrHdr     = correlationId != null ? correlationId : "unknown";
            URI    uri         = Objects.requireNonNull(URI.create(url), "uri");

            RequestBodySpec spec = restClient
                .method(HttpMethod.valueOf(method))
                .uri(uri)
                .contentType(MediaType.parseMediaType(contentType))
                .header("X-Correlation-ID",  corrHdr)
                .header("Authorization",      jwtTokenProvider.bearerHeader());

            request.getHeaders().forEach((k, v) -> spec.header(k, Objects.requireNonNull(v, k)));

            ResponseEntity<String> response = spec
                .body(Objects.requireNonNull(body, "body"))
                .retrieve()
                .onStatus(status -> !status.is2xxSuccessful(),
                    (req, res) -> {
                        throw new ExternalServiceException(
                            ErrorCode.EXTERNAL_SERVICE_ERROR,
                            url,
                            "HTTP " + res.getStatusCode().value() + " from " + url,
                            correlationId,
                            res.getStatusCode().value(),
                            null);
                    })
                .toEntity(String.class);

            long duration = System.currentTimeMillis() - startTime;
            int  status   = response.getStatusCode().value();

            log.info("httpOutboundCompleted", correlationId,
                "service",    request.getServiceName(),
                "url",        url,
                "httpStatus", status,
                "durationMs", duration,
                "attempt",    attempt);

            return HttpOutboundResponse.builder()
                .statusCode(status)
                .body(response.getBody())
                .success(true)
                .build();

        } catch (ExternalServiceException e) {
            long duration = System.currentTimeMillis() - startTime;
            log.warn("httpOutboundAttemptFailed", correlationId,
                "service",    request.getServiceName(),
                "url",        request.getUrl(),
                "httpStatus", e.getHttpStatusCode(),
                "durationMs", duration,
                "attempt",    attempt,
                "error",      e.getMessage());
            throw e;

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            log.warn("httpOutboundConnectionFailed", correlationId, e,
                "service",    request.getServiceName(),
                "url",        request.getUrl(),
                "durationMs", duration,
                "attempt",    attempt,
                "error",      e.getMessage());
            throw ExternalServiceException.unavailable(request.getUrl(), correlationId, e);
        }
    }

    private int resolveMaxAttempts(HttpOutboundRequest request) {
        Integer override = request.getMaxRetryAttempts();
        return override != null ? override : httpClientProperties.getRetry().getMaxAttempts();
    }

    private boolean isRetryable(Integer httpStatus) {
        if (httpStatus == null || httpStatus == 0) {
            return true; // connection-level failure — no HTTP response received
        }
        List<Integer> retryable = httpClientProperties.getRetry().getRetryableStatusCodes();
        return retryable.contains(httpStatus);
    }

    private long nextDelay(long currentDelayMs) {
        double multiplier = httpClientProperties.getRetry().getBackoffMultiplier();
        long   maxDelay   = httpClientProperties.getRetry().getMaxDelayMs();
        return Math.min((long) (currentDelayMs * multiplier), maxDelay);
    }

    private void sleep(long ms, String correlationId) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("httpOutboundRetryInterrupted", correlationId, "delayMs", ms);
        }
    }
}
