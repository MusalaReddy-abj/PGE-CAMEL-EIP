package com.pge.krakencis.routes.rcdc.request;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.SoapEnvelopeExtractorProcessor;
import com.pge.krakencis.processors.SoapEnvelopeWrapperProcessor;
import com.pge.krakencis.processors.rcdc.request.RcdcAcknowledgementProcessor;
import com.pge.krakencis.processors.rcdc.request.RcdcRequestProcessor;
import com.pge.krakencis.processors.rcdc.request.RcdcTargetMappingProcessor;
import com.pge.krakencis.routes.BaseRoute;
import org.apache.camel.converter.jaxb.JaxbDataFormat;
import org.apache.camel.model.rest.RestBindingMode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class RcdcRequestHttpListner extends BaseRoute {

    private static final String OPERATION = "postRCDC";

    private final SoapEnvelopeExtractorProcessor soapEnvelopeExtractorProcessor;
    private final SoapEnvelopeWrapperProcessor   soapEnvelopeWrapperProcessor;
    private final RcdcRequestProcessor           rcdcRequestProcessor;
    private final RcdcTargetMappingProcessor     rcdcTargetMappingProcessor;
    private final RcdcAcknowledgementProcessor   rcdcAcknowledgementProcessor;

    @Value("${kafka.topic.rcdc:kraken-rcdc-events}")
    private String rcdcTopic;

    public RcdcRequestHttpListner(CorrelationIdProcessor         correlationIdProcessor,
                                   RouteLoggingProcessor          routeLoggingProcessor,
                                   RouteExceptionProcessor        exceptionProcessor,
                                   SoapEnvelopeExtractorProcessor soapEnvelopeExtractorProcessor,
                                   SoapEnvelopeWrapperProcessor   soapEnvelopeWrapperProcessor,
                                   RcdcRequestProcessor           rcdcRequestProcessor,
                                   RcdcTargetMappingProcessor     rcdcTargetMappingProcessor,
                                   RcdcAcknowledgementProcessor   rcdcAcknowledgementProcessor) {
        super(correlationIdProcessor, routeLoggingProcessor, exceptionProcessor);
        this.soapEnvelopeExtractorProcessor = soapEnvelopeExtractorProcessor;
        this.soapEnvelopeWrapperProcessor   = soapEnvelopeWrapperProcessor;
        this.rcdcRequestProcessor           = rcdcRequestProcessor;
        this.rcdcTargetMappingProcessor     = rcdcTargetMappingProcessor;
        this.rcdcAcknowledgementProcessor   = rcdcAcknowledgementProcessor;
    }

    @Override
    public void configure() {
        JaxbDataFormat jaxbFormat = new JaxbDataFormat("com.pge.krakencis.models.rcdc.request");

        rest()
            .post("/rcdc")
                .description("Receive a Remote Connect / Disconnect Command and publish to Kafka")
                .consumes("text/xml")
                .produces("text/xml")
                .bindingMode(RestBindingMode.off)
                .to("direct:process-rcdc");

        processingRoute("direct:process-rcdc", "route-post-rcdc", OPERATION, route ->
            route
                .process(com.pge.krakencis.logging.SpanEnricher.httpRoute("POST", "/api/v1/rcdc"))
                // OpenTelemetry Integration — RECEIVED stage span (child of POST /rcdc).
                // Trace visible in Jaeger. No business logic changes.
                .process(com.pge.krakencis.observability.TracingHelper.stage(
                        com.pge.krakencis.observability.TracingConstants.SPAN_RECEIVED))
                .process(soapEnvelopeExtractorProcessor)  // strip SOAP envelope → plain requestMessage
                .unmarshal(jaxbFormat)
                .process(rcdcRequestProcessor)
                // OpenTelemetry Integration — VALIDATED stage span (validation succeeded above).
                // No business logic changes.
                .process(com.pge.krakencis.observability.TracingHelper.stage(
                        com.pge.krakencis.observability.TracingConstants.SPAN_VALIDATED))
                .process(rcdcTargetMappingProcessor)
                .setProperty(LogConstants.KAFKA_TOPIC, constant(rcdcTopic))
                // OpenTelemetry Integration — KAFKA_PUBLISH_START stage span.
                // No business logic changes.
                .process(com.pge.krakencis.observability.TracingHelper.stage(
                        com.pge.krakencis.observability.TracingConstants.SPAN_KAFKA_PUBLISH_START))
                .to("direct:publishToKafka")
                // OpenTelemetry Integration — KAFKA_PUBLISH_SUCCESS stage span.
                // No business logic changes.
                .process(com.pge.krakencis.observability.TracingHelper.stage(
                        com.pge.krakencis.observability.TracingConstants.SPAN_KAFKA_PUBLISH_SUCCESS))
                .process(rcdcAcknowledgementProcessor)
                .process(soapEnvelopeWrapperProcessor)    // wrap acknowledgement in SOAP envelope
                // OpenTelemetry Integration — RESPONSE_SENT stage span (SOAP ack built).
                // Trace visible in Jaeger. No business logic changes.
                .process(com.pge.krakencis.observability.TracingHelper.stage(
                        com.pge.krakencis.observability.TracingConstants.SPAN_RESPONSE_SENT))
        );
    }
}
