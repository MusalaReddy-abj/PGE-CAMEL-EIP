package com.pge.krakencis.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pge.krakencis.configs.HttpClientProperties;
import com.pge.krakencis.configs.KafkaProducerConfig;
import com.pge.krakencis.exceptions.ExternalServiceException;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.OutboundRequestEvent;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Publishes failed outbound HTTP requests to Kafka for deferred reprocessing or
 * manual investigation.
 *
 * <h3>Topics</h3>
 * <ul>
 *   <li><b>Retry queue</b> ({@code http.client.retry-topic}) — receives transient
 *       failures after all in-process retries are exhausted. A separate retry-queue
 *       consumer (not part of this service) can re-attempt the HTTP call after a
 *       configurable cooling-off period. On final failure the consumer should forward
 *       the message to the DLQ.</li>
 *   <li><b>DLQ</b> ({@code http.client.dlq-topic}) — receives permanent failures:
 *       non-retryable HTTP 4xx errors and messages that have also exhausted the retry
 *       queue. Requires manual investigation and replay.</li>
 * </ul>
 *
 * <p>Publish failures are logged but <em>never re-thrown</em> so that a Kafka
 * outage cannot mask the original service error received by the caller.
 */
@Component
public class DlqPublisher {

    private static final StructuredLogger log = StructuredLogger.of(DlqPublisher.class);

    private final ProducerTemplate     producerTemplate;
    private final KafkaProducerConfig  kafkaProducerConfig;
    private final HttpClientProperties httpClientProperties;
    private final ObjectMapper         objectMapper;

    public DlqPublisher(ProducerTemplate     producerTemplate,
                        KafkaProducerConfig  kafkaProducerConfig,
                        HttpClientProperties httpClientProperties,
                        ObjectMapper         objectMapper) {
        this.producerTemplate     = producerTemplate;
        this.kafkaProducerConfig  = kafkaProducerConfig;
        this.httpClientProperties = httpClientProperties;
        this.objectMapper         = objectMapper;
    }

    /**
     * Publishes to the retry queue after in-process retries fail on a transient error.
     * A downstream consumer should re-attempt the request after a delay.
     */
    public void publishToRetryQueue(HttpOutboundRequest      request,
                                    ExternalServiceException error,
                                    String                   correlationId,
                                    int                      attempt,
                                    int                      maxAttempts) {
        publish("RETRY", httpClientProperties.getRetryTopic(),
                request, error, correlationId, attempt, maxAttempts);
    }

    /**
     * Publishes to the DLQ for non-retryable (4xx) errors or permanently failed requests.
     * Requires manual investigation.
     */
    public void publishToDlq(HttpOutboundRequest      request,
                              ExternalServiceException error,
                              String                   correlationId,
                              int                      attempt,
                              int                      maxAttempts) {
        publish("DLQ", httpClientProperties.getDlqTopic(),
                request, error, correlationId, attempt, maxAttempts);
    }

    // ── private ───────────────────────────────────────────────────────────────

    private void publish(String                   eventType,
                          String                   topic,
                          HttpOutboundRequest      request,
                          ExternalServiceException error,
                          String                   correlationId,
                          int                      attempt,
                          int                      maxAttempts) {
        try {
            OutboundRequestEvent event = OutboundRequestEvent.builder()
                .eventType(eventType)
                .correlationId(correlationId)
                .serviceName(request.getServiceName())
                .url(request.getUrl())
                .method(request.getMethod())
                .contentType(request.getContentType())
                .body(request.getBody())
                .attempt(attempt)
                .maxAttempts(maxAttempts)
                .httpStatus(error != null && error.getHttpStatusCode() != null
                    ? error.getHttpStatusCode() : 0)
                .errorType(error != null ? error.getClass().getSimpleName() : null)
                .errorMessage(error != null ? error.getMessage() : null)
                .timestamp(Instant.now().toString())
                .build();

            String json     = objectMapper.writeValueAsString(event);
            String kafkaUri = kafkaProducerConfig.buildUri(topic);

            producerTemplate.sendBodyAndHeader(
                kafkaUri, json, KafkaConstants.KEY, correlationId);

            log.info("publishedTo" + eventType, correlationId,
                "topic",       topic,
                "attempt",     attempt,
                "maxAttempts", maxAttempts,
                "url",         request.getUrl());

        } catch (Exception e) {
            log.error("failedToPublishTo" + eventType, correlationId, e,
                "topic", topic,
                "url",   request.getUrl());
        }
    }
}
