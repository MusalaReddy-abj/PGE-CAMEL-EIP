package com.pge.krakencis.routes.alarms;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.SpanEnricher;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.models.alarms.AlarmRequest;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.alarms.AlarmEventProcessor;
import com.pge.krakencis.processors.alarms.AlarmResponseProcessor;
import com.pge.krakencis.routes.BaseRoute;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * HTTP listener for inbound alarm events from Kraken CIS.
 *
 * <p>Exposes {@code POST /api/v1/alarms}. The body must be a valid
 * {@link AlarmRequest} JSON document. Validated events are mapped to
 * {@link com.pge.krakencis.models.KrakenEvent} objects and published to
 * the configured Kafka alarm topic.
 *
 * <h3>Route ID</h3>
 * {@code route-post-alarms}
 */

@Component
public class AlarmHttpListner extends BaseRoute {

    private static final String OPERATION = "postAlarms";

    private final AlarmEventProcessor    alarmEventProcessor;
    private final AlarmResponseProcessor alarmResponseProcessor;

    @Value("${kafka.topic.alarms}")
    private String alarmsTopic;

    public AlarmHttpListner(CorrelationIdProcessor  correlationIdProcessor,
                            RouteLoggingProcessor   routeLoggingProcessor,
                            RouteExceptionProcessor exceptionProcessor,
                            AlarmEventProcessor     alarmEventProcessor,
                            AlarmResponseProcessor  alarmResponseProcessor) {
        super(correlationIdProcessor, routeLoggingProcessor, exceptionProcessor);
        this.alarmEventProcessor    = alarmEventProcessor;
        this.alarmResponseProcessor = alarmResponseProcessor;
    }

    @Override
    public void configure() {
        rest()
            .post("/alarms")
                .description("Receive alarm events from Kraken CIS and publish to Kafka")
                .consumes("application/json")
                .produces("application/json")
                .type(AlarmRequest.class)
                .to("direct:process-alarms");

        processingRoute("direct:process-alarms", "route-post-alarms", OPERATION, route ->
            route
                .process(SpanEnricher.httpRoute("POST", "/api/v1/alarms"))
                .process(alarmEventProcessor)
                .setProperty(LogConstants.KAFKA_TOPIC, constant(alarmsTopic))
                .to("direct:publishToKafka")
                .process(alarmResponseProcessor)
        );
    }
}
