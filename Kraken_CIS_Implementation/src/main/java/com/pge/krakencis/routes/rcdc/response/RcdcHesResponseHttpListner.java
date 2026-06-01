package com.pge.krakencis.routes.rcdc.response;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.models.rcdc.response.RcdcHesResponseMessage;
import com.pge.krakencis.processors.rcdc.response.RcdcHesAckProcessor;
import com.pge.krakencis.processors.rcdc.response.RcdcHesResponseProcessor;
import com.pge.krakencis.routes.BaseRoute;
import org.apache.camel.model.rest.RestParamType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class RcdcHesResponseHttpListner extends BaseRoute {

    private static final String OPERATION = "postRCDCHESResponse";

    private final RcdcHesResponseProcessor rcdcHesResponseProcessor;
    private final RcdcHesAckProcessor      rcdcHesAckProcessor;

    @Value("${kafka.topic.rcdc-hes-response:kraken-rcdc-hes-response-events}")
    private String rcdcHesResponseTopic;

    public RcdcHesResponseHttpListner(RcdcHesResponseProcessor rcdcHesResponseProcessor,
                                 RcdcHesAckProcessor      rcdcHesAckProcessor) {
        this.rcdcHesResponseProcessor = rcdcHesResponseProcessor;
        this.rcdcHesAckProcessor      = rcdcHesAckProcessor;
    }

    @Override
    public void configure() {
        rest("/rcdc/response")
            .post()
                .description("Receive RCDC response from HES and publish to Kafka")
                .consumes("application/json")
                .produces("application/json")
                .param()
                    .name("X-Correlation-ID").type(RestParamType.header)
                    .required(false)
                .endParam()
                .type(RcdcHesResponseMessage.class)
                .to("direct:process-rcdc-hes-response");

        processingRoute("direct:process-rcdc-hes-response",
                        "route-post-rcdc-hes-response", OPERATION, route ->
            route
                .process(rcdcHesResponseProcessor)
                .setProperty(LogConstants.KAFKA_TOPIC, constant(rcdcHesResponseTopic))
                .to("direct:publishToKafka")
                .process(rcdcHesAckProcessor)
        );
    }
}
