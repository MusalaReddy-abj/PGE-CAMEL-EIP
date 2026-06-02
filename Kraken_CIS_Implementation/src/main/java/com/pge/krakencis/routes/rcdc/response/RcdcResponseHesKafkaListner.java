package com.pge.krakencis.routes.rcdc.response;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.rcdc.response.RcdcMdmNotificationProcessor;
import com.pge.krakencis.routes.BaseKafkaConsumerRoute;
import org.apache.camel.model.RouteDefinition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer route that forwards HES response events to the MDM notification service.
 *
 * <p>Consumes from {@code kafka.topic.rcdc-hes-response} using a dedicated
 * consumer group ({@code groupId}-mdm) and calls the MDM SOAP endpoint via
 * {@link RcdcMdmNotificationProcessor}.
 *
 * <h3>Error routing</h3>
 * Inherited from {@link BaseKafkaConsumerRoute}:
 * <ul>
 *   <li>Payload errors / SOAP Client fault → DLQ immediately</li>
 *   <li>4xx client errors → DLQ immediately</li>
 *   <li>5xx / 404 / network / SOAP Server fault → retry 3× → retry queue</li>
 *   <li>Unknown exceptions → retry 3× → retry queue</li>
 * </ul>
 *
 * <h3>Route ID</h3>
 * {@code route-rcdc-hes-kafka-consumer}
 */
@Component
public class RcdcResponseHesKafkaListner extends BaseKafkaConsumerRoute {

    private static final String OPERATION    = "consumeRcdcHesResponse";
    private static final String SERVICE_NAME = "SOA-MDM-Service";

    private final RcdcMdmNotificationProcessor rcdcMdmNotificationProcessor;

    @Value("${kafka.topic.rcdc-hes-retry:kraken-rcdc-hes-retry-events}")
    private String hesRetryTopic;

    @Value("${kafka.topic.rcdc-hes-dlq:kraken-rcdc-hes-dlq-events}")
    private String hesDlqTopic;

    public RcdcResponseHesKafkaListner(CorrelationIdProcessor       correlationIdProcessor,
                                       RouteLoggingProcessor        routeLoggingProcessor,
                                       RouteExceptionProcessor      exceptionProcessor,
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
            + "&autoCommitEnable=false&allowManualCommit=true"
            + "&concurrentConsumers={{kafka.consumer.concurrent-consumers:1}}"
            + "&maxPollIntervalMs={{kafka.consumer.max-poll-interval-ms:300000}}"
            + "&heartbeatIntervalMs={{kafka.consumer.heartbeat-interval-ms:10000}}";

        RouteDefinition route = from(uri).routeId("route-rcdc-hes-kafka-consumer");

        configureKafkaErrorHandlers(route, hesRetryTopic, hesDlqTopic, SERVICE_NAME);

        route
            .process(exchange -> exchange.setProperty(
                LogConstants.PROP_ORIGINAL_BODY, exchange.getIn().getBody(String.class)))
            .process(exchange -> extractCorrelationId(exchange, "rcdcHesMessageConsumed"))
            .process(routeLoggingProcessor.entry(OPERATION))
            .process(rcdcMdmNotificationProcessor)
            .process(this::commitOffset)
            .process(routeLoggingProcessor.exit(OPERATION));
    }
}
