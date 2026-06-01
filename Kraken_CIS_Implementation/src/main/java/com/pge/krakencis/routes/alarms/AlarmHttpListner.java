package com.pge.krakencis.routes.alarms;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.models.alarms.AlarmRequest;
import com.pge.krakencis.processors.alarms.AlarmEventProcessor;
import com.pge.krakencis.processors.alarms.AlarmResponseProcessor;
import com.pge.krakencis.routes.BaseRoute;
import org.apache.camel.model.rest.RestParamType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AlarmHttpListner extends BaseRoute {

    private static final String OPERATION = "postAlarms";

    private final AlarmEventProcessor    alarmEventProcessor;
    private final AlarmResponseProcessor alarmResponseProcessor;

    @Value("${kafka.topic.alarms}")
    private String alarmsTopic;

    public AlarmHttpListner(AlarmEventProcessor    alarmEventProcessor,
                      AlarmResponseProcessor alarmResponseProcessor) {
        this.alarmEventProcessor    = alarmEventProcessor;
        this.alarmResponseProcessor = alarmResponseProcessor;
    }

    @Override
    public void configure() {

        rest()
            .post("/{version}/alarms")
                .description("Versioned alarm submission endpoint")
                .consumes("application/json")
                .produces("application/json")
                .param()
                    .name("version").type(RestParamType.path)
                    .description("API version identifier, e.g. v1, v2, v3")
                    .required(true)
                .endParam()
                .param()
                    .name("X-Correlation-ID").type(RestParamType.header)
                    .description("Optional correlation ID for request tracing (passed from Kong)")
                    .required(false)
                .endParam()
                .type(AlarmRequest.class)
                .to("direct:process-alarms");

        rest()
            .post("/alarms")
                .description("Alarm submission endpoint (current stable version)")
                .consumes("application/json")
                .produces("application/json")
                .param()
                    .name("X-Correlation-ID").type(RestParamType.header)
                    .description("Optional correlation ID for request tracing (passed from Kong)")
                    .required(false)
                .endParam()
                .type(AlarmRequest.class)
                .to("direct:process-alarms");

        processingRoute("direct:process-alarms", "route-post-alarms", OPERATION, route ->
            route
                .choice()
                    .when(header("version").isNull())
                        .setHeader("API-Version", constant("v2"))
                    .otherwise()
                        .setHeader("API-Version", header("version"))
                .end()
                .process(alarmEventProcessor)
                .setProperty(LogConstants.KAFKA_TOPIC, constant(alarmsTopic))
                .to("direct:publishToKafka")
                .process(alarmResponseProcessor)
        );
    }
}
