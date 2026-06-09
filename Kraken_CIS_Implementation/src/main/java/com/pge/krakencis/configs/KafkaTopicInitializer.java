package com.pge.krakencis.configs;

import com.pge.krakencis.logging.StructuredLogger;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Creates every Kafka topic the application uses on startup, if it does not already
 * exist. Required because AWS MSK has auto-topic-creation disabled — without this,
 * publishes to a non-existent topic fail (and, in the file flow, are silently
 * swallowed → files archived with no data).
 *
 * <p>Idempotent and best-effort: topics that already exist are skipped, and any
 * failure is logged without aborting startup (the topics may already exist, or be
 * created out-of-band by infra). Disable with {@code kafka.topic.auto-create=false}.
 *
 * <h3>Replication factor</h3>
 * The configured {@code kafka.topic.replication-factor} (default 3) is capped at the
 * actual broker count, so a single-broker local cluster gets RF=1 and a 3-broker MSK
 * cluster gets RF=3 — no manual per-environment tuning needed.
 */
@Component
@ConditionalOnProperty(prefix = "kafka.topic", name = "auto-create", havingValue = "true", matchIfMissing = true)
public class KafkaTopicInitializer {

    private static final StructuredLogger log = StructuredLogger.of(KafkaTopicInitializer.class);
    private static final int TIMEOUT_S = 15;

    @Value("${kafka.topic.partitions:3}")          private int   partitions;
    @Value("${kafka.topic.replication-factor:3}")  private short replicationFactor;

    @Value("${kafka.producer.brokers:localhost:9092}")                private String brokers;
    @Value("${kafka.producer.security-protocol:PLAINTEXT}")           private String securityProtocol;
    @Value("${kafka.producer.sasl-mechanism:}")                       private String saslMechanism;
    @Value("${kafka.producer.sasl-jaas-config:}")                     private String saslJaasConfig;
    @Value("${kafka.producer.sasl-client-callback-handler-class:}")   private String saslCallbackHandlerClass;

    // All topics the app produces to / consumes from.
    @Value("${kafka.topic.alarms:kraken-alarm-events}")                            private String alarms;
    @Value("${kafka.topic.rcdc:kraken-rcdc-events}")                               private String rcdc;
    @Value("${kafka.topic.rcdc-retry:kraken-rcdc-retry-events}")                   private String rcdcRetry;
    @Value("${kafka.topic.rcdc-dlq:kraken-rcdc-dlq-events}")                       private String rcdcDlq;
    @Value("${kafka.topic.rcdc-parking:kraken-rcdc-parking-events}")               private String rcdcParking;
    @Value("${kafka.topic.rcdc-hes-response:kraken-rcdc-hes-response-events}")     private String hesResponse;
    @Value("${kafka.topic.rcdc-hes-retry:kraken-rcdc-hes-retry-events}")           private String hesRetry;
    @Value("${kafka.topic.rcdc-hes-dlq:kraken-rcdc-hes-dlq-events}")               private String hesDlq;
    @Value("${kafka.topic.rcdc-hes-parking:kraken-rcdc-hes-parking-events}")       private String hesParking;
    @Value("${kafka.topic.profile-reads:kraken-profile-reads-events}")            private String profileReads;
    @Value("${kafka.topic.profile-reads-dlq:kraken-profile-reads-dlq-events}")    private String profileReadsDlq;
    @Value("${kafka.topic.profile-reads-parking:kraken-profile-reads-parking-events}") private String profileReadsParking;
    @Value("${kafka.topic.profile-reads-audit:kraken-profile-reads-audit-events}") private String profileReadsAudit;

    @PostConstruct
    public void ensureTopics() {
        List<String> topics = List.of(
            alarms, rcdc, rcdcRetry, rcdcDlq, rcdcParking,
            hesResponse, hesRetry, hesDlq, hesParking,
            profileReads, profileReadsDlq, profileReadsParking, profileReadsAudit);

        try (AdminClient admin = AdminClient.create(adminProps())) {
            Set<String> existing = admin.listTopics().names().get(TIMEOUT_S, TimeUnit.SECONDS);

            // Cap replication factor at the actual broker count (local=1, MSK=3).
            int brokerCount = admin.describeCluster().nodes().get(TIMEOUT_S, TimeUnit.SECONDS).size();
            short rf = (short) Math.min(replicationFactor, Math.max(1, brokerCount));

            List<NewTopic> toCreate = new ArrayList<>();
            for (String t : topics) {
                if (!existing.contains(t)) {
                    toCreate.add(new NewTopic(t, partitions, rf));
                }
            }

            if (toCreate.isEmpty()) {
                log.info("kafkaTopicsAllPresent", null, "topicCount", topics.size());
                return;
            }

            CreateTopicsResult result = admin.createTopics(toCreate);
            for (Map.Entry<String, ?> e : result.values().entrySet()) {
                try {
                    result.values().get(e.getKey()).get(TIMEOUT_S, TimeUnit.SECONDS);
                    log.info("kafkaTopicCreated", null,
                        "topic", e.getKey(), "partitions", partitions, "replicationFactor", rf);
                } catch (ExecutionException ex) {
                    if (ex.getCause() instanceof TopicExistsException) {
                        log.debug("kafkaTopicAlreadyExists", null, "topic", e.getKey());
                    } else {
                        log.error("kafkaTopicCreateFailed", null, ex, "topic", e.getKey());
                    }
                }
            }
        } catch (Exception e) {
            // Best-effort: never block startup on topic creation.
            log.error("kafkaTopicInitFailed", null, e, "brokers", brokers);
        }
    }

    private Properties adminProps() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,      brokers);
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,     String.valueOf(TIMEOUT_S * 1000));
        p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(TIMEOUT_S * 1000));
        if (!"PLAINTEXT".equalsIgnoreCase(securityProtocol)) {
            p.put("security.protocol", securityProtocol);
            if (!saslMechanism.isBlank())            p.put("sasl.mechanism", saslMechanism);
            if (!saslJaasConfig.isBlank())           p.put("sasl.jaas.config", saslJaasConfig);
            if (!saslCallbackHandlerClass.isBlank()) p.put("sasl.client.callback.handler.class", saslCallbackHandlerClass);
        }
        return p;
    }
}
