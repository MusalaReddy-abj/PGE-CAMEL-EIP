package com.pge.krakencis.configs;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Configures retry behaviour, DLQ topic, and retry-queue topic for all outbound
 * HTTP calls made via {@link com.pge.krakencis.services.HttpClientService}.
 *
 * <p>Global defaults are applied to every service call. Individual service endpoints
 * can override {@code maxRetryAttempts} via
 * {@link ExternalServiceProperties.ServiceEndpoint#getMaxRetryAttempts()}.
 *
 * <pre>
 * http:
 *   client:
 *     retry:
 *       max-attempts: 3
 *       initial-delay-ms: 1000
 *       backoff-multiplier: 2.0
 *       max-delay-ms: 30000
 *       retryable-status-codes: [429, 500, 502, 503, 504]
 * </pre>
 *
 * <p>Retry and DLQ Kafka topic names are NOT configured here. Each Kafka consumer
 * route ({@link com.pge.krakencis.routes.BaseKafkaConsumerRoute}) passes its own
 * flow-specific retry and DLQ topics directly via {@code @Value} injection.
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "http.client")
public class HttpClientProperties {

    private RetryConfig retry = new RetryConfig();

    @Data
    public static class RetryConfig {

        /** Maximum number of attempts (1 = no retry). */
        private int    maxAttempts          = 3;

        /** Delay before the first retry. Doubles on each subsequent attempt. */
        private long   initialDelayMs       = 1_000;

        /** Multiplier applied to the delay after each failed attempt. */
        private double backoffMultiplier    = 2.0;

        /** Upper bound on the inter-retry delay regardless of multiplier. */
        private long   maxDelayMs           = 30_000;

        /**
         * HTTP status codes eligible for retry.
         *
         * <ul>
         *   <li>404 — service endpoint temporarily unavailable (deployment / routing issue)</li>
         *   <li>408 — request timeout</li>
         *   <li>429 — rate limited; back off and retry</li>
         *   <li>500/502/503/504 — server-side transient errors</li>
         * </ul>
         */
        private List<Integer> retryableStatusCodes = List.of(404, 408, 429, 500, 502, 503, 504);
    }
}
