package com.pge.krakencis.routes.admin;

import com.pge.krakencis.logging.StructuredLogger;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestBindingMode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Camel REST route that exposes a read-only DLQ visibility endpoint.
 *
 * <h3>Endpoint</h3>
 * <pre>GET /api/v1/admin/dlq/stats</pre>
 *
 * <h3>Response</h3>
 * <pre>
 * {
 *   "topics": [
 *     { "topic": "kraken-rcdc-dlq-events",          "consumerGroup": "kraken-dlq-monitor", "lag": 5 },
 *     { "topic": "kraken-rcdc-hes-dlq-events",      "consumerGroup": "kraken-dlq-monitor", "lag": 0 },
 *     { "topic": "kraken-profile-reads-dlq-events", "consumerGroup": "kraken-dlq-monitor", "lag": 0 }
 *   ],
 *   "checkedAt": "2025-12-18T10:00:00+05:30"
 * }
 * </pre>
 *
 * <p>Lag = {@code latestOffset - committedOffset} summed across all partitions
 * for the dedicated monitor consumer group {@code kraken-dlq-monitor}. This group
 * never actually consumes messages — its committed offset tracks how many messages
 * operations teams have inspected or replayed.
 */
@Component
public class DlqStatsRoute extends RouteBuilder {

    private static final StructuredLogger log            = StructuredLogger.of(DlqStatsRoute.class);
    private static final String           CONSUMER_GROUP = "kraken-dlq-monitor";
    private static final int              TIMEOUT_S      = 10;
    static final String                   METRIC_DLQ_LAG = "kafka.dlq.lag";

    @Value("${kafka.producer.brokers:localhost:9092}")
    private String bootstrapServers;

    @Value("${kafka.producer.security-protocol:PLAINTEXT}")
    private String securityProtocol;

    @Value("${kafka.producer.sasl-mechanism:}")
    private String saslMechanism;

    @Value("${kafka.producer.sasl-jaas-config:}")
    private String saslJaasConfig;

    @Value("${kafka.producer.sasl-client-callback-handler-class:}")
    private String saslCallbackHandlerClass;

    @Value("${kafka.topic.rcdc-dlq:kraken-rcdc-dlq-events}")
    private String rcdcDlqTopic;

    @Value("${kafka.topic.rcdc-hes-dlq:kraken-rcdc-hes-dlq-events}")
    private String rcdcHesDlqTopic;

    @Value("${kafka.topic.profile-reads-dlq:kraken-profile-reads-dlq-events}")
    private String profileReadsDlqTopic;

    private final MeterRegistry meterRegistry;

    // AtomicLong per DLQ topic — updated after every /stats call, read by Prometheus scrapes.
    private final AtomicLong rcdcDlqLag        = new AtomicLong(0);
    private final AtomicLong rcdcHesDlqLag     = new AtomicLong(0);
    private final AtomicLong profileReadsDlqLag = new AtomicLong(0);

    public DlqStatsRoute(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void registerGauges() {
        meterRegistry.gauge(METRIC_DLQ_LAG,
            Tags.of("topic", rcdcDlqTopic.isEmpty() ? "kraken-rcdc-dlq-events" : rcdcDlqTopic),
            rcdcDlqLag, AtomicLong::get);
        meterRegistry.gauge(METRIC_DLQ_LAG,
            Tags.of("topic", rcdcHesDlqTopic.isEmpty() ? "kraken-rcdc-hes-dlq-events" : rcdcHesDlqTopic),
            rcdcHesDlqLag, AtomicLong::get);
        meterRegistry.gauge(METRIC_DLQ_LAG,
            Tags.of("topic", profileReadsDlqTopic.isEmpty() ? "kraken-profile-reads-dlq-events" : profileReadsDlqTopic),
            profileReadsDlqLag, AtomicLong::get);
    }

    @Override
    public void configure() {
        rest("/admin/dlq")
            .tag("Admin")
            .description("DLQ visibility")
            .get("/stats")
                .description("Message-lag summary for all DLQ topics")
                .produces("application/json")
                .bindingMode(RestBindingMode.off)
                .to("direct:dlq-stats");

        from("direct:dlq-stats")
            .routeId("route-dlq-stats")
            .process(this::buildDlqStats);
    }

    private void buildDlqStats(Exchange exchange) {
        List<String> topics = List.of(rcdcDlqTopic, rcdcHesDlqTopic, profileReadsDlqTopic);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,     String.valueOf(TIMEOUT_S * 1_000));
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(TIMEOUT_S * 1_000));

        if (!"PLAINTEXT".equalsIgnoreCase(securityProtocol)) {
            props.put("security.protocol",                    securityProtocol);
            if (!saslMechanism.isBlank())          props.put("sasl.mechanism",                        saslMechanism);
            if (!saslJaasConfig.isBlank())         props.put("sasl.jaas.config",                      saslJaasConfig);
            if (!saslCallbackHandlerClass.isBlank()) props.put("sasl.client.callback.handler.class", saslCallbackHandlerClass);
        }

        try (AdminClient admin = AdminClient.create(props)) {
            List<Map<String, Object>> stats    = computeLags(admin, topics);
            String                   checkedAt = OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            String                   json      = toJson(stats, checkedAt);

            exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 200);
            exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/json");
            exchange.getIn().setBody(json);

            log.info("dlqStatsRetrieved", null,
                "consumerGroup", CONSUMER_GROUP, "topicCount", topics.size());

        } catch (Exception e) {
            log.error("dlqStatsRetrievalFailed", null, e, "brokers", bootstrapServers);
            exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 500);
            exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/json");
            exchange.getIn().setBody(
                "{\"error\":\"Failed to retrieve DLQ stats: " + esc(e.getMessage()) + "\"}");
        }
    }

    private List<Map<String, Object>> computeLags(AdminClient admin,
                                                   List<String> topics) throws Exception {
        // ── 1. Collect latest (end) offset for every partition ────────────────
        Map<TopicPartition, OffsetSpec> latestReq = new HashMap<>();
        for (String topic : topics) {
            List<TopicPartitionInfo> parts = admin.describeTopics(List.of(topic))
                .allTopicNames().get(TIMEOUT_S, TimeUnit.SECONDS)
                .get(topic).partitions();
            for (TopicPartitionInfo tpi : parts) {
                latestReq.put(new TopicPartition(topic, tpi.partition()), OffsetSpec.latest());
            }
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
            admin.listOffsets(latestReq).all().get(TIMEOUT_S, TimeUnit.SECONDS);

        // ── 2. Fetch committed offsets for the monitor consumer group ─────────
        ListConsumerGroupOffsetsResult cgResult = admin.listConsumerGroupOffsets(CONSUMER_GROUP);
        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed =
            cgResult.partitionsToOffsetAndMetadata().get(TIMEOUT_S, TimeUnit.SECONDS);

        // ── 3. Sum lag per topic and update Prometheus gauges ────────────────
        List<Map<String, Object>> results = new ArrayList<>();
        for (String topic : topics) {
            long lag = 0L;
            for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e
                    : latestOffsets.entrySet()) {
                if (!e.getKey().topic().equals(topic)) continue;
                long end = e.getValue().offset();
                long cmt = committed.getOrDefault(e.getKey(), null) != null
                    ? committed.get(e.getKey()).offset() : 0L;
                lag += Math.max(0L, end - cmt);
            }
            updateLagGauge(topic, lag);
            results.add(Map.of("topic", topic, "consumerGroup", CONSUMER_GROUP, "lag", lag));
        }
        return results;
    }

    private String toJson(List<Map<String, Object>> stats, String checkedAt) {
        StringBuilder sb = new StringBuilder("{\n  \"topics\": [\n");
        for (int i = 0; i < stats.size(); i++) {
            Map<String, Object> s = stats.get(i);
            sb.append("    { \"topic\": \"").append(esc(s.get("topic").toString()))
              .append("\", \"consumerGroup\": \"").append(esc(s.get("consumerGroup").toString()))
              .append("\", \"lag\": ").append(s.get("lag")).append(" }");
            if (i < stats.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append("  ],\n  \"checkedAt\": \"").append(esc(checkedAt)).append("\"\n}");
        return sb.toString();
    }

    private void updateLagGauge(String topic, long lag) {
        if (topic.equals(rcdcDlqTopic))         rcdcDlqLag.set(lag);
        else if (topic.equals(rcdcHesDlqTopic)) rcdcHesDlqLag.set(lag);
        else if (topic.equals(profileReadsDlqTopic)) profileReadsDlqLag.set(lag);
    }

    private static String esc(String v) {
        if (v == null) return "";
        return v.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r");
    }
}
