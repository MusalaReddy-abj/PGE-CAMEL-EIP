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
 *     dlq-topic:   kraken-http-outbound-dlq
 *     retry-topic: kraken-http-outbound-retry
 * </pre>
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "http.client")
public class HttpClientProperties {

    private RetryConfig retry      = new RetryConfig();
    private String      dlqTopic   = "kraken-http-outbound-dlq";
    private String      retryTopic = "kraken-http-outbound-retry";

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
         * HTTP status codes that are eligible for retry.
         * 4xx codes (except 429) are intentionally absent — client errors
         * will not succeed on retry.
         */
        private List<Integer> retryableStatusCodes = List.of(429, 500, 502, 503, 504);
    }
}
