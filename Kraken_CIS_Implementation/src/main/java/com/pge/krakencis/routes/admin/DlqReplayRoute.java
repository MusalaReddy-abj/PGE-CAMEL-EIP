package com.pge.krakencis.routes.admin;

import com.pge.krakencis.logging.StructuredLogger;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Automatic, safe Dead-Letter-Queue replay.
 *
 * <p><b>Opt-in</b> — created only when {@code dlq.replay.enabled=true}. On a fixed
 * schedule it drains a bounded number of messages from each DLQ topic and decides,
 * per message, what to do based on the error headers already attached by
 * {@link com.pge.krakencis.routes.KafkaErrorRoutes}:
 *
 * <pre>
 * DLQ message
 *   ├─ permanent error (X-Error-Type = Validation/Transformation, X-Error-Http-Status 4xx)
 *   │      → PARKING topic (can never succeed — needs a human)
 *   └─ transient error (5xx / 0 / RetryableException, etc.)
 *         ├─ X-Replay-Count &lt; max  → republish to SOURCE topic (full reprocess),
 *         │                            X-Replay-Count incremented, X-Error-* stripped
 *         └─ X-Replay-Count ≥ max  → PARKING topic (gave up)
 * </pre>
 *
 * <h3>Why these safeguards</h3>
 * Blindly replaying a DLQ creates an infinite poison-pill loop (a permanent error
 * fails → DLQ → replay → fails …) and can hammer a downstream. Transient-only +
 * a replay cap + a parking topic make replay safe and self-terminating. The PARKING
 * topic becomes the true "needs attention" terminal; the DLQ becomes staging.
 *
 * <h3>Config (all under {@code dlq.replay})</h3>
 * {@code enabled} (default false) · {@code interval-ms} (default 30 min) ·
 * {@code max-messages-per-run} (default 100) · {@code max-replays} (default 3).
 */
@Component
@ConditionalOnProperty(prefix = "dlq.replay", name = "enabled", havingValue = "true")
public class DlqReplayRoute extends RouteBuilder {

    private static final StructuredLogger log = StructuredLogger.of(DlqReplayRoute.class);
    private static final String REPLAY_GROUP   = "kraken-dlq-replay";
    private static final String HDR_REPLAY     = "X-Replay-Count";
    private static final String HDR_CORR       = "X-Correlation-ID";
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    @Value("${dlq.replay.interval-ms:1800000}")        private long intervalMs;
    @Value("${dlq.replay.initial-delay-ms:30000}")     private long initialDelayMs;
    @Value("${dlq.replay.max-messages-per-run:100}")   private int  maxPerRun;
    @Value("${dlq.replay.max-replays:3}")              private int  maxReplays;

    @Value("${kafka.producer.brokers:localhost:9092}") private String brokers;
    @Value("${kafka.producer.security-protocol:PLAINTEXT}")            private String securityProtocol;
    @Value("${kafka.producer.sasl-mechanism:}")                        private String saslMechanism;
    @Value("${kafka.producer.sasl-jaas-config:}")                      private String saslJaasConfig;
    @Value("${kafka.producer.sasl-client-callback-handler-class:}")    private String saslCallbackHandlerClass;

    // DLQ → (source, parking) topic mapping
    @Value("${kafka.topic.rcdc-dlq:kraken-rcdc-dlq-events}")                 private String rcdcDlq;
    @Value("${kafka.topic.rcdc:kraken-rcdc-events}")                        private String rcdcSource;
    @Value("${kafka.topic.rcdc-parking:kraken-rcdc-parking-events}")        private String rcdcParking;

    @Value("${kafka.topic.rcdc-hes-dlq:kraken-rcdc-hes-dlq-events}")         private String hesDlq;
    @Value("${kafka.topic.rcdc-hes-response:kraken-rcdc-hes-response-events}") private String hesSource;
    @Value("${kafka.topic.rcdc-hes-parking:kraken-rcdc-hes-parking-events}") private String hesParking;

    @Value("${kafka.topic.profile-reads-dlq:kraken-profile-reads-dlq-events}")     private String prDlq;
    @Value("${kafka.topic.profile-reads:kraken-profile-reads-events}")             private String prSource;
    @Value("${kafka.topic.profile-reads-parking:kraken-profile-reads-parking-events}") private String prParking;

    @Override
    public void configure() {
        // First run after initialDelayMs (default 30 s), then every intervalMs.
        log.info("dlqReplayRouteStarting", null,
            "initialDelayMs", initialDelayMs, "intervalMs", intervalMs, "maxPerRun", maxPerRun);
        from("timer:dlq-replay?period=" + intervalMs + "&delay=" + initialDelayMs)
            .routeId("route-dlq-replay")
            .process(this::replayAllDlqs);
    }

    private void replayAllDlqs(Exchange exchange) {
        // Heartbeat — proves the scheduler is alive even when all DLQs are empty.
        log.info("dlqReplayTick", null, "maxPerRun", maxPerRun, "maxReplays", maxReplays);
        // RCDC and HES DLQs are message-level (original message preserved) → replayable.
        replayDlq(rcdcDlq, rcdcSource, rcdcParking, true);
        replayDlq(hesDlq,  hesSource,  hesParking,  true);
        // Profile Reads DLQ is row-level (malformed CSV rows, not transient outages) and the
        // events topic has a different shape — replaying would push bad data onto the stream.
        // Park-only: every entry goes straight to parking for manual/data review.
        replayDlq(prDlq,   prSource,   prParking,   false);
    }

    /**
     * Drains up to {@code maxPerRun} messages from one DLQ topic and routes each.
     *
     * @param replayable when {@code false}, every message is parked (never sent to the
     *                   source topic) — used for row-level / bad-data DLQs.
     */
    private void replayDlq(String dlqTopic, String sourceTopic, String parkingTopic, boolean replayable) {
        int replayed = 0, parked = 0, scanned = 0;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps());
             KafkaProducer<String, String>  producer = new KafkaProducer<>(producerProps())) {

            consumer.subscribe(List.of(dlqTopic));

            while (scanned < maxPerRun) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                if (records.isEmpty()) break;   // nothing left this run

                for (ConsumerRecord<String, String> rec : records) {
                    if (scanned >= maxPerRun) break;
                    scanned++;

                    String cid = headerString(rec, HDR_CORR, rec.key());

                    // Park-only DLQ (row-level / bad data): never replay to source.
                    if (!replayable) {
                        producer.send(buildRecord(parkingTopic, rec, false));
                        parked++;
                        log.info("dlqParkedRowLevel", cid,
                            "dlqTopic", dlqTopic, "parkingTopic", parkingTopic,
                            "reason", "row-level DLQ — not replayed to source");
                        continue;
                    }

                    if (isPermanent(rec)) {
                        producer.send(buildRecord(parkingTopic, rec, false));
                        parked++;
                        log.warn("dlqReplayParkedPermanent", cid,
                            "dlqTopic", dlqTopic, "parkingTopic", parkingTopic,
                            "errorType", headerString(rec, "X-Error-Type", "unknown"));
                        continue;
                    }

                    int replayCount = headerInt(rec, HDR_REPLAY, 0);
                    if (replayCount >= maxReplays) {
                        producer.send(buildRecord(parkingTopic, rec, false));
                        parked++;
                        log.warn("dlqReplayParkedExhausted", cid,
                            "dlqTopic", dlqTopic, "parkingTopic", parkingTopic,
                            "replayCount", replayCount, "maxReplays", maxReplays);
                    } else {
                        producer.send(buildRecord(sourceTopic, rec, true));
                        replayed++;
                        log.info("dlqReplayed", cid,
                            "dlqTopic", dlqTopic, "sourceTopic", sourceTopic,
                            "replayCount", replayCount + 1);
                    }
                }
            }

            producer.flush();
            if (scanned > 0) consumer.commitSync();

            if (scanned > 0) {
                log.info("dlqReplayRunCompleted", null,
                    "dlqTopic", dlqTopic, "scanned", scanned,
                    "replayed", replayed, "parked", parked);
            }
        } catch (Exception e) {
            log.error("dlqReplayFailed", null, e, "dlqTopic", dlqTopic);
        }
    }

    /**
     * Builds the outgoing record. When {@code replay} is true the message is being
     * sent back to the source topic: strip the {@code X-Error-*} / {@code X-Destination-Type}
     * headers and increment {@code X-Replay-Count}. When parking, preserve the error
     * headers as-is for human inspection.
     */
    private ProducerRecord<String, String> buildRecord(String topic,
                                                       ConsumerRecord<String, String> src,
                                                       boolean replay) {
        ProducerRecord<String, String> out = new ProducerRecord<>(topic, src.key(), src.value());
        for (Header h : src.headers()) {
            String k = h.key();
            if (replay && (k.startsWith("X-Error-")
                    || k.equals("X-Destination-Type")
                    || k.equals(HDR_REPLAY)
                    // Drop the stale W3C trace context from the original failure. The Agent
                    // re-injects the current (replay-run) traceparent on send(), so the
                    // reprocessed message links to this replay — not an ancient trace.
                    || k.equalsIgnoreCase("traceparent")
                    || k.equalsIgnoreCase("tracestate")
                    || k.equalsIgnoreCase("baggage"))) {
                continue;   // drop error + stale trace context on replay
            }
            out.headers().add(h);
        }
        if (replay) {
            int next = headerInt(src, HDR_REPLAY, 0) + 1;
            out.headers().add(new RecordHeader(HDR_REPLAY,
                String.valueOf(next).getBytes(StandardCharsets.UTF_8)));
        }
        return out;
    }

    /** Permanent = payload/4xx error that can never succeed on replay. */
    private boolean isPermanent(ConsumerRecord<String, String> rec) {
        String type = headerString(rec, "X-Error-Type", "");
        if (type.equals("ValidationException") || type.equals("TransformationException")) {
            return true;
        }
        int status = headerInt(rec, "X-Error-Http-Status", 0);
        return status >= 400 && status < 500;   // client error — permanent
    }

    // ── Kafka client config ───────────────────────────────────────────────────

    private Properties consumerProps() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  brokers);
        p.put(ConsumerConfig.GROUP_ID_CONFIG,           REPLAY_GROUP);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,   maxPerRun);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        applySecurity(p);
        return p;
    }

    private Properties producerProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        applySecurity(p);
        return p;
    }

    private void applySecurity(Properties p) {
        if ("PLAINTEXT".equalsIgnoreCase(securityProtocol)) return;
        p.put("security.protocol", securityProtocol);
        if (!saslMechanism.isBlank())            p.put("sasl.mechanism", saslMechanism);
        if (!saslJaasConfig.isBlank())           p.put("sasl.jaas.config", saslJaasConfig);
        if (!saslCallbackHandlerClass.isBlank()) p.put("sasl.client.callback.handler.class", saslCallbackHandlerClass);
    }

    // ── Header helpers (Kafka headers are byte[] on the wire) ──────────────────

    private String headerString(ConsumerRecord<String, String> rec, String key, String dflt) {
        Header h = rec.headers().lastHeader(key);
        if (h == null || h.value() == null) return dflt;
        return new String(h.value(), StandardCharsets.UTF_8);
    }

    private int headerInt(ConsumerRecord<String, String> rec, String key, int dflt) {
        String v = headerString(rec, key, null);
        if (v == null) return dflt;
        try { return Integer.parseInt(v.trim()); } catch (NumberFormatException e) { return dflt; }
    }
}
