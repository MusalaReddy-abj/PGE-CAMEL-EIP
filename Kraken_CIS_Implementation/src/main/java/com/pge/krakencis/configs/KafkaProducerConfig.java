package com.pge.krakencis.configs;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

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
     * Query-string-only parameters (no topic prefix).
     * Used with Camel's .toD() for dynamic topic resolution at runtime:
     *   .toD("kafka:${exchangeProperty.kafkaTopic}?" + buildQueryString())
     */
    public String buildQueryString() {
        return "brokers="          + brokers
            + "&acks="             + acks
            + "&retries="          + retries
            + "&requestTimeoutMs=" + requestTimeoutMs
            + "&lingerMs="         + lingerMs
            + "&compressionCodec=" + compressionType
            + "&keySerializer="    + keySerializer
            + "&valueSerializer="  + valueSerializer;
    }

    /**
     * Full URI for a known topic (used in non-dynamic routes).
     */
    public String buildUri(String topic) {
        return "kafka:" + topic + "?" + buildQueryString();
    }
}
