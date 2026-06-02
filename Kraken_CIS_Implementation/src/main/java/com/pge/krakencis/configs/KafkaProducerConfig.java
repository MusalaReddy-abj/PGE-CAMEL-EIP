package com.pge.krakencis.configs;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Kafka producer settings bound from the {@code kafka.producer} configuration prefix.
 *
 * <p>The two main uses are:
 * <ul>
 *   <li>{@link #buildQueryString()} — appends all producer options as a Camel URI
 *       query string for use with {@code .toD()} and dynamic topic routing.</li>
 *   <li>{@link #buildUri(String topic)} — constructs a full Camel Kafka URI for a
 *       known topic. The topic name is validated to reject reserved URI characters
 *       ({@code ?}, {@code &}, {@code =}) that could inject unintended endpoint
 *       options.</li>
 * </ul>
 *
 * <p>Defaults are set for local development. Override per environment in
 * {@code config/{profile}/kafka.yml}.
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "kafka.producer")
public class KafkaProducerConfig {

    private String brokers       = "localhost:9092";
    private String acks          = "all";
    private int    retries       = 3;
    private int    requestTimeoutMs = 30000;
    private int    lingerMs      = 5;
    private String compressionType = "lz4";
    private String keySerializer   =
        "org.apache.kafka.common.serialization.StringSerializer";
    private String valueSerializer =
        "org.apache.kafka.common.serialization.StringSerializer";
    /**
     * Max in-flight requests per connection. Must be ≤ 5 when {@code acks=all}
     * and {@code retries > 0} to guarantee ordering and prevent duplicates under
     * high-throughput retry scenarios.
     */
    private int    maxInFlightRequest = 5;

    /**
     * Query-string-only parameters (no topic prefix).
     * Used with Camel's .toD() for dynamic topic resolution at runtime:
     *   .toD("kafka:${exchangeProperty.kafkaTopic}?" + buildQueryString())
     */
    public String buildQueryString() {
        return "brokers="                  + brokers
            + "&acks="                     + acks
            + "&retries="                  + retries
            + "&requestTimeoutMs="         + requestTimeoutMs
            + "&lingerMs="                 + lingerMs
            + "&compressionCodec="         + compressionType
            + "&keySerializer="            + keySerializer
            + "&valueSerializer="          + valueSerializer
            + "&maxInFlightRequest="       + maxInFlightRequest;
    }

    /**
     * Full URI for a known topic (used in non-dynamic routes).
     */
    public String buildUri(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Kafka topic name must not be blank");
        }
        if (topic.chars().anyMatch(c -> c == '?' || c == '&' || c == '=')) {
            throw new IllegalArgumentException(
                "Kafka topic name contains reserved URI characters: " + topic);
        }
        return "kafka:" + topic + "?" + buildQueryString();
    }
}
