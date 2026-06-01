package com.pge.krakencis.routes.rcdc.request;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.rcdc.request.RcdcTargetCallProcessor;
import com.pge.krakencis.processors.rcdc.request.RcdcTargetResponseProcessor;
import com.pge.krakencis.routes.BaseRoute;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Kafka consumer route for inbound RCDC (Remote Connect/Disconnect Command) events.
 *
 * <p>Consumes messages from the {@code kafka.topic.rcdc} topic (default:
 * {@code kraken-rcdc-events}), forwards each command to the downstream SOA-RCDC
 * service via {@link com.pge.krakencis.processors.rcdc.request.RcdcTargetCallProcessor},
 * and validates the HTTP response via
 * {@link com.pge.krakencis.processors.rcdc.request.RcdcTargetResponseProcessor}.
 *
 * <h3>Offset commit strategy</h3>
 * <p>Auto-commit is disabled ({@code autoCommitEnable=false}). The Kafka offset is
 * committed unconditionally inside a {@code doFinally} block so that:
 * <ul>
 *   <li>Successful messages are acknowledged after processing completes.</li>
 *   <li>Failed messages are <em>skipped</em> (offset advanced) after the error is
 *       logged, preventing infinite poison-pill redelivery.</li>
 * </ul>
 *
 * <h3>Correlation ID</h3>
 * <p>The correlation ID is read from the Kafka message key. If the message has no
 * key, a random UUID is generated so that every exchange is fully traceable.
 *
 * <h3>Route ID</h3>
 * {@code route-rcdc-kafka-consumer}
 */
@Component
public class RcdcRequestKafkaListner extends BaseRoute {

    private static final StructuredLogger log       = StructuredLogger.of(RcdcRequestKafkaListner.class);
    private static final String           OPERATION = "consumeRcdcRequest";

    private final RcdcTargetCallProcessor     rcdcTargetCallProcessor;
    private final RcdcTargetResponseProcessor rcdcTargetResponseProcessor;

    public RcdcRequestKafkaListner(CorrelationIdProcessor     correlationIdProcessor,
                                   RouteLoggingProcessor      routeLoggingProcessor,
                                   RouteExceptionProcessor    exceptionProcessor,
                                   RcdcTargetCallProcessor    rcdcTargetCallProcessor,
                                   RcdcTargetResponseProcessor rcdcTargetResponseProcessor) {
        super(correlationIdProcessor, routeLoggingProcessor, exceptionProcessor);
        this.rcdcTargetCallProcessor     = rcdcTargetCallProcessor;
        this.rcdcTargetResponseProcessor = rcdcTargetResponseProcessor;
    }

    @Override
    public void configure() {
        final String uri =
            "kafka:{{kafka.topic.rcdc:kraken-rcdc-events}}"
            + "?brokers={{kafka.producer.brokers}}"
            + "&groupId={{kafka.consumer.group-id:kraken-cis-group}}"
            + "&autoOffsetReset={{kafka.consumer.auto-offset-reset:earliest}}"
            + "&maxPollRecords={{kafka.consumer.max-poll-records:500}}"
            + "&autoCommitEnable=false&allowManualCommit=true";

        from(uri).routeId("route-rcdc-kafka-consumer")
            .process(this::extractCorrelationId)
            .process(routeLoggingProcessor.entry(OPERATION))
            .doTry()
                .process(rcdcTargetCallProcessor)
                .process(rcdcTargetResponseProcessor)
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
        log.info("rcdcMessageConsumed", cid,
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
