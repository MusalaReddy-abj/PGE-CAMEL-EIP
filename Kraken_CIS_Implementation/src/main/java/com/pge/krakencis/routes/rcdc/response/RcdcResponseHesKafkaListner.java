package com.pge.krakencis.routes.rcdc.response;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.rcdc.response.RcdcMdmNotificationProcessor;
import com.pge.krakencis.routes.BaseRoute;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Kafka consumer route that forwards HES (Head-End System) response events to the
 * MDM (Meter Data Management) notification service.
 *
 * <p>Consumes from the {@code kafka.topic.rcdc-hes-response} topic (default:
 * {@code kraken-rcdc-hes-response-events}) using a dedicated consumer group
 * ({@code {kafka.consumer.group-id}-mdm}) so this consumer is independent of
 * any other consumers on the same topic.
 *
 * <h3>Processing steps</h3>
 * <ol>
 *   <li>Extract / generate a correlation ID from the Kafka message key.</li>
 *   <li>Delegate to {@link com.pge.krakencis.processors.rcdc.response.RcdcMdmNotificationProcessor},
 *       which calls the MDM SOAP endpoint via
 *       {@link com.pge.krakencis.services.SOAMDMNotificationService}.</li>
 *   <li>Commit the Kafka offset unconditionally in {@code doFinally} — errors are
 *       logged and skipped rather than replayed indefinitely.</li>
 * </ol>
 *
 * <h3>Route ID</h3>
 * {@code route-rcdc-hes-kafka-consumer}
 */
@Component
public class RcdcResponseHesKafkaListner extends BaseRoute {

    private static final StructuredLogger log       = StructuredLogger.of(RcdcResponseHesKafkaListner.class);
    private static final String           OPERATION = "consumeRcdcHesResponse";

    private final RcdcMdmNotificationProcessor rcdcMdmNotificationProcessor;

    public RcdcResponseHesKafkaListner(CorrelationIdProcessor      correlationIdProcessor,
                                       RouteLoggingProcessor       routeLoggingProcessor,
                                       RouteExceptionProcessor     exceptionProcessor,
                                       RcdcMdmNotificationProcessor rcdcMdmNotificationProcessor) {
        super(correlationIdProcessor, routeLoggingProcessor, exceptionProcessor);
        this.rcdcMdmNotificationProcessor = rcdcMdmNotificationProcessor;
    }

    @Override
    public void configure() {
        final String uri =
            "kafka:{{kafka.topic.rcdc-hes-response:kraken-rcdc-hes-response-events}}"
            + "?brokers={{kafka.producer.brokers}}"
            + "&groupId={{kafka.consumer.group-id:kraken-cis-group}}-mdm"
            + "&autoOffsetReset={{kafka.consumer.auto-offset-reset:earliest}}"
            + "&maxPollRecords={{kafka.consumer.max-poll-records:500}}"
            + "&autoCommitEnable=false&allowManualCommit=true";

        from(uri).routeId("route-rcdc-hes-kafka-consumer")
            .process(this::extractCorrelationId)
            .process(routeLoggingProcessor.entry(OPERATION))
            .doTry()
                .process(rcdcMdmNotificationProcessor)
            .doCatch(Exception.class)
                .process(exceptionProcessor.system(OPERATION))
            .doFinally()
                .process(this::commitOffset)
                .process(routeLoggingProcessor.exit(OPERATION))
            .end();
    }

    private void extractCorrelationId(Exchange exchange) {
        String cid = exchange.getIn().getHeader(KafkaConstants.KEY, String.class);
        if (cid == null || cid.isBlank()) {
            cid = UUID.randomUUID().toString();
        }
        exchange.setProperty(LogConstants.PROP_CORRELATION_ID, cid);
        exchange.getIn().setHeader("X-Correlation-ID", cid);
        log.info("rcdcHesMessageConsumed", cid,
            "topic",  exchange.getIn().getHeader(KafkaConstants.TOPIC),
            "offset", exchange.getIn().getHeader(KafkaConstants.OFFSET));
    }

    private void commitOffset(Exchange exchange) {
        KafkaManualCommit commit = exchange.getIn().getHeader(
            KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        String cid = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        if (commit != null) {
            commit.commit();
            log.debug("kafkaOffsetCommitted", cid);
        } else {
            log.warn("kafkaManualCommitHeaderMissing", cid);
        }
    }
}
