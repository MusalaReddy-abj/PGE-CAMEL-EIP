package com.pge.krakencis.routes.rcdc.request;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.processors.rcdc.request.RcdcAcknowledgementProcessor;
import com.pge.krakencis.processors.rcdc.request.RcdcRequestProcessor;
import com.pge.krakencis.processors.rcdc.request.RcdcTargetMappingProcessor;
import com.pge.krakencis.routes.BaseRoute;
import org.apache.camel.converter.jaxb.JaxbDataFormat;
import org.apache.camel.model.rest.RestBindingMode;
import org.apache.camel.model.rest.RestParamType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class RcdcRequestHttpListner extends BaseRoute {

    private static final String OPERATION = "postRCDC";

    private final RcdcRequestProcessor       rcdcRequestProcessor;
    private final RcdcTargetMappingProcessor rcdcTargetMappingProcessor;
    private final RcdcAcknowledgementProcessor rcdcAcknowledgementProcessor;

    @Value("${kafka.topic.rcdc:kraken-rcdc-events}")
    private String rcdcTopic;

    public RcdcRequestHttpListner(RcdcRequestProcessor         rcdcRequestProcessor,
                     RcdcTargetMappingProcessor   rcdcTargetMappingProcessor,
                     RcdcAcknowledgementProcessor rcdcAcknowledgementProcessor) {
        this.rcdcRequestProcessor         = rcdcRequestProcessor;
        this.rcdcTargetMappingProcessor   = rcdcTargetMappingProcessor;
        this.rcdcAcknowledgementProcessor = rcdcAcknowledgementProcessor;
    }

    @Override
    public void configure() {
        JaxbDataFormat jaxbFormat = new JaxbDataFormat("com.pge.krakencis.models.rcdc.request");

        rest()
            .post("/{version}/rcdc")
                .description("Versioned Remote Connect / Disconnect Command")
                .consumes("application/xml")
                .produces("application/xml")
                .bindingMode(RestBindingMode.off)
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
                .to("direct:process-rcdc");

        rest()
            .post("/rcdc")
                .description("Remote Connect / Disconnect Command (current stable version)")
                .consumes("application/xml")
                .produces("application/xml")
                .bindingMode(RestBindingMode.off)
                .param()
                    .name("X-Correlation-ID").type(RestParamType.header)
                    .description("Optional correlation ID for request tracing (passed from Kong)")
                    .required(false)
                .endParam()
                .to("direct:process-rcdc");

        processingRoute("direct:process-rcdc", "route-post-rcdc", OPERATION, route ->
            route
                .choice()
                    .when(header("version").isNull())
                        .setHeader("API-Version", constant("v2"))
                    .otherwise()
                        .setHeader("API-Version", header("version"))
                .end()
                .unmarshal(jaxbFormat)
                .process(rcdcRequestProcessor)
                .process(rcdcTargetMappingProcessor)
                .setProperty(LogConstants.KAFKA_TOPIC, constant(rcdcTopic))
                .to("direct:publishToKafka")
                .process(rcdcAcknowledgementProcessor)
        );
    }
}
