package com.pge.krakencis.routes.rcdc.request;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.SpanEnricher;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.rcdc.request.RcdcTargetCallProcessor;
import com.pge.krakencis.processors.rcdc.request.RcdcTargetResponseProcessor;
import com.pge.krakencis.routes.BaseKafkaConsumerRoute;
import org.apache.camel.model.RouteDefinition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer route for inbound RCDC (Remote Connect/Disconnect Command) events.
 *
 * <p>Consumes from {@code kafka.topic.rcdc} and forwards each command to the
 * downstream SOA-RCDC HTTP service via {@link RcdcTargetCallProcessor}.
 *
 * <h3>Error routing</h3>
 * Inherited from {@link BaseKafkaConsumerRoute}:
 * <ul>
 *   <li>Payload errors → DLQ immediately</li>
 *   <li>4xx client errors → DLQ immediately</li>
 *   <li>5xx / 404 / network (RetryableException) → retry 3× → retry queue</li>
 *   <li>Unknown exceptions → retry 3× → retry queue</li>
 * </ul>
 *
 * <h3>Route ID</h3>
 * {@code route-rcdc-kafka-consumer}
 */
@Component
public class RcdcRequestKafkaListner extends BaseKafkaConsumerRoute {

    private static final String OPERATION    = "consumeRcdcRequest";
    private static final String SERVICE_NAME = "SOA-RCDC-TargetService";

    private final RcdcTargetCallProcessor     rcdcTargetCallProcessor;
    private final RcdcTargetResponseProcessor rcdcTargetResponseProcessor;

    @Value("${kafka.topic.rcdc-retry:kraken-rcdc-retry-events}")
    private String rcdcRetryTopic;

    @Value("${kafka.topic.rcdc-dlq:kraken-rcdc-dlq-events}")
    private String rcdcDlqTopic;

    public RcdcRequestKafkaListner(CorrelationIdProcessor      correlationIdProcessor,
                                   RouteLoggingProcessor       routeLoggingProcessor,
                                   RouteExceptionProcessor     exceptionProcessor,
                                   RcdcTargetCallProcessor     rcdcTargetCallProcessor,
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
            + "&autoCommitEnable=false&allowManualCommit=true"
            + "&consumersCount={{kafka.consumer.consumers-count:1}}"
            + "&maxPollIntervalMs={{kafka.consumer.max-poll-interval-ms:300000}}"
            + "&heartbeatIntervalMs={{kafka.consumer.heartbeat-interval-ms:10000}}"
            + securityQueryString();

        RouteDefinition route = from(uri).routeId("route-rcdc-kafka-consumer");

        configureKafkaErrorHandlers(route, rcdcRetryTopic, rcdcDlqTopic, SERVICE_NAME);

        route
            .process(SpanEnricher.kafkaConsume())
            .process(exchange -> exchange.setProperty(
                LogConstants.PROP_ORIGINAL_BODY, exchange.getIn().getBody(String.class)))
            .process(exchange -> extractCorrelationId(exchange, "rcdcMessageConsumed"))
            .process(routeLoggingProcessor.entry(OPERATION))
            .process(rcdcTargetCallProcessor)
            .process(rcdcTargetResponseProcessor)
            .process(this::commitOffset)
            .process(routeLoggingProcessor.exit(OPERATION));
    }
}
