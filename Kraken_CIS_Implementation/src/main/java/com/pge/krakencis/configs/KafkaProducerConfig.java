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

    // ── Security (optional — only appended when securityProtocol is not PLAINTEXT) ──
    /** e.g. {@code SASL_SSL} for MSK IAM/SCRAM, {@code PLAINTEXT} for local dev. */
    private String securityProtocol              = "PLAINTEXT";
    /** e.g. {@code AWS_MSK_IAM} or {@code SCRAM-SHA-512}. */
    private String saslMechanism                 = "";
    /** Full JAAS config line, e.g. {@code software.amazon.msk.auth.iam.IAMLoginModule required;}. */
    private String saslJaasConfig                = "";
    /** Callback handler class, e.g. {@code software.amazon.msk.auth.iam.IAMClientCallbackHandler}. */
    private String saslClientCallbackHandlerClass = "";

    /**
     * Query-string-only parameters (no topic prefix).
     * Used with Camel's .toD() for dynamic topic resolution at runtime:
     *   .toD("kafka:${exchangeProperty.kafkaTopic}?" + buildQueryString())
     */
    public String buildQueryString() {
        StringBuilder q = new StringBuilder()
            .append("brokers=")                        .append(brokers)
            .append("&additionalProperties[acks]=")    .append(acks)
            .append("&retries=")                       .append(retries)
            .append("&requestTimeoutMs=")              .append(requestTimeoutMs)
            .append("&lingerMs=")                      .append(lingerMs)
            .append("&compressionCodec=")              .append(compressionType)
            .append("&keySerializer=")                 .append(keySerializer)
            .append("&valueSerializer=")               .append(valueSerializer)
            .append("&maxInFlightRequest=")            .append(maxInFlightRequest);

        if (!"PLAINTEXT".equalsIgnoreCase(securityProtocol)) {
            q.append("&securityProtocol=")                   .append(securityProtocol);
            if (!saslMechanism.isBlank()) {
                q.append("&saslMechanism=")                  .append(saslMechanism);
            }
            if (!saslJaasConfig.isBlank()) {
                q.append("&saslJaasConfig=")                 .append(saslJaasConfig);
            }
            if (!saslClientCallbackHandlerClass.isBlank()) {
                // saslClientCallbackHandlerClass is not a valid Camel Kafka URI param —
                // must be passed via additionalProperties using the native Kafka property name.
                q.append("&additionalProperties[sasl.client.callback.handler.class]=")
                 .append(saslClientCallbackHandlerClass);
            }
        }
        return q.toString();
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
