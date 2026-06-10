package com.pge.krakencis.routes.odr;

import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.SoapEnvelopeExtractorProcessor;
import com.pge.krakencis.processors.SoapEnvelopeWrapperProcessor;
import com.pge.krakencis.processors.odr.OdrMockCallProcessor;
import com.pge.krakencis.processors.odr.OdrRequestProcessor;
import com.pge.krakencis.routes.BaseRoute;
import org.apache.camel.converter.jaxb.JaxbDataFormat;
import org.apache.camel.model.rest.RestBindingMode;
import org.springframework.stereotype.Component;

@Component
public class OdrHttpListner extends BaseRoute {

    private static final String OPERATION = "postOnDemandRead";

    private final SoapEnvelopeExtractorProcessor soapEnvelopeExtractorProcessor;
    private final SoapEnvelopeWrapperProcessor   soapEnvelopeWrapperProcessor;
    private final OdrRequestProcessor            odrRequestProcessor;
    private final OdrMockCallProcessor           odrMockCallProcessor;

    public OdrHttpListner(CorrelationIdProcessor         correlationIdProcessor,
                          RouteLoggingProcessor          routeLoggingProcessor,
                          RouteExceptionProcessor        exceptionProcessor,
                          SoapEnvelopeExtractorProcessor soapEnvelopeExtractorProcessor,
                          SoapEnvelopeWrapperProcessor   soapEnvelopeWrapperProcessor,
                          OdrRequestProcessor            odrRequestProcessor,
                          OdrMockCallProcessor           odrMockCallProcessor) {
        super(correlationIdProcessor, routeLoggingProcessor, exceptionProcessor);
        this.soapEnvelopeExtractorProcessor = soapEnvelopeExtractorProcessor;
        this.soapEnvelopeWrapperProcessor   = soapEnvelopeWrapperProcessor;
        this.odrRequestProcessor            = odrRequestProcessor;
        this.odrMockCallProcessor           = odrMockCallProcessor;
    }

    @Override
    public void configure() {
        JaxbDataFormat jaxbFormat = new JaxbDataFormat("com.pge.krakencis.models.odr.request");

        rest()
            .post("/v1/odr")
                .description("Receive an OnDemandRead request and forward to the mock SOAP service")
                .consumes("text/xml")
                .produces("text/xml")
                .bindingMode(RestBindingMode.off)
                .to("direct:process-odr");

        processingRoute("direct:process-odr", "route-post-odr", OPERATION, route ->
            route
                .process(com.pge.krakencis.logging.SpanEnricher.httpRoute("POST", "/api/v1/odr"))
                .setProperty("odr.rawXml", simple("${body}"))  // preserve full SOAP envelope for mock forwarding
                .process(soapEnvelopeExtractorProcessor)         // strip envelope → body becomes requestMessage
                .unmarshal(jaxbFormat)
                .process(odrRequestProcessor)
                .process(odrMockCallProcessor)                   // sets body to mock response XML
                .process(soapEnvelopeWrapperProcessor)           // wrap response in SOAP envelope
        );
    }
}
