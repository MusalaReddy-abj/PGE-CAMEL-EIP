package com.pge.krakencis.routes.odr;

import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.SoapEnvelopeExtractorProcessor;
import com.pge.krakencis.processors.SoapEnvelopeWrapperProcessor;
import com.pge.krakencis.processors.odr.OdrMockCallProcessor;
import com.pge.krakencis.processors.odr.OdrRequestProcessor;
import com.pge.krakencis.processors.odr.OdrV2ResponseProcessor;
import com.pge.krakencis.routes.BaseRoute;
import org.apache.camel.converter.jaxb.JaxbDataFormat;
import org.apache.camel.model.rest.RestBindingMode;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * ODR <b>v2</b> HTTP listener — {@code POST /api/v2/odr}.
 *
 * <p>Coexists with v1 ({@link OdrHttpListner}, {@code /api/v1/odr}). Same XML request
 * contract, same SOAP/XML response format, same downstream mock call — v2 only makes an
 * <b>additive</b> change: it appends a new {@code <apiVersion>} field to the response
 * (see {@link OdrV2ResponseProcessor}). Additive-only, so it is backward compatible.
 *
 * <h3>Versioning / rollback</h3>
 * Gated by {@code odr.v2.enabled} (default {@code true}). Roll back to v1-only by setting
 * {@code odr.v2.enabled=false} and restarting — no code change, v1 unaffected. Future
 * versions can be added the same way ({@code /v3/odr}), each independently toggleable.
 *
 * <h3>Route ID</h3>
 * {@code route-post-odr-v2}
 */
@Component
@ConditionalOnProperty(prefix = "odr.v2", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OdrV2HttpListner extends BaseRoute {

    private static final String OPERATION = "postOnDemandReadV2";

    private final SoapEnvelopeExtractorProcessor soapEnvelopeExtractorProcessor;
    private final SoapEnvelopeWrapperProcessor   soapEnvelopeWrapperProcessor;
    private final OdrRequestProcessor            odrRequestProcessor;
    private final OdrMockCallProcessor           odrMockCallProcessor;
    private final OdrV2ResponseProcessor         odrV2ResponseProcessor;

    public OdrV2HttpListner(CorrelationIdProcessor         correlationIdProcessor,
                            RouteLoggingProcessor          routeLoggingProcessor,
                            RouteExceptionProcessor        exceptionProcessor,
                            SoapEnvelopeExtractorProcessor soapEnvelopeExtractorProcessor,
                            SoapEnvelopeWrapperProcessor   soapEnvelopeWrapperProcessor,
                            OdrRequestProcessor            odrRequestProcessor,
                            OdrMockCallProcessor           odrMockCallProcessor,
                            OdrV2ResponseProcessor         odrV2ResponseProcessor) {
        super(correlationIdProcessor, routeLoggingProcessor, exceptionProcessor);
        this.soapEnvelopeExtractorProcessor = soapEnvelopeExtractorProcessor;
        this.soapEnvelopeWrapperProcessor   = soapEnvelopeWrapperProcessor;
        this.odrRequestProcessor            = odrRequestProcessor;
        this.odrMockCallProcessor           = odrMockCallProcessor;
        this.odrV2ResponseProcessor         = odrV2ResponseProcessor;
    }

    @Override
    public void configure() {
        JaxbDataFormat jaxbFormat = new JaxbDataFormat("com.pge.krakencis.models.odr.request");

        rest()
            .post("/v2/odr")
                .description("ODR v2 — same SOAP/XML request and response as v1, plus an additive apiVersion field")
                .consumes("text/xml")
                .produces("text/xml")
                .bindingMode(RestBindingMode.off)
                .to("direct:process-odr-v2");

        // Identical pipeline to v1; the only difference is the v2 response enricher that
        // appends the new field before the SOAP envelope is wrapped.
        processingRoute("direct:process-odr-v2", "route-post-odr-v2", OPERATION, route ->
            route
                .process(com.pge.krakencis.logging.SpanEnricher.httpRoute("POST", "/api/v2/odr"))
                .setProperty("odr.rawXml", simple("${body}"))  // preserve full SOAP envelope for mock forwarding
                .process(soapEnvelopeExtractorProcessor)         // strip envelope → body becomes requestMessage
                .unmarshal(jaxbFormat)
                .process(odrRequestProcessor)
                .process(odrMockCallProcessor)                   // body = mock response XML
                .process(odrV2ResponseProcessor)                 // v2 additive change: append <apiVersion>
                .process(soapEnvelopeWrapperProcessor)           // wrap result in SOAP envelope (same as v1)
        );
    }
}
