package com.pge.krakencis.processors.odr;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

/**
 * Applies the ODR <b>v2</b> response difference: an <b>additive</b>, backward-compatible
 * change over v1 — the same SOAP/XML response plus a new {@code <apiVersion>} field.
 *
 * <p>Runs after {@link OdrMockCallProcessor} (body = mock response XML) and before
 * {@link com.pge.krakencis.processors.SoapEnvelopeWrapperProcessor}, so the new element is
 * appended as a sibling inside the SOAP {@code <Body>}. Additive-only: nothing in the v1
 * response is removed or renamed, so existing clients are unaffected.
 */
@Component
public class OdrV2ResponseProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(OdrV2ResponseProcessor.class);

    /** The v2-only field value; also surfaced as a response header for visibility. */
    private static final String API_VERSION = "2.0";

    @Override
    public void process(Exchange exchange) {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String responseXml   = exchange.getIn().getBody(String.class);
        if (responseXml == null) {
            responseXml = "";
        }

        // Additive change: keep the entire v1 response, append the new field after it.
        // The SoapEnvelopeWrapperProcessor (next step) wraps the result in the SOAP body.
        String enriched = responseXml + "\n<apiVersion>" + API_VERSION + "</apiVersion>";

        exchange.getIn().setBody(enriched);
        exchange.getIn().setHeader("X-API-Version", API_VERSION);

        log.info("odrV2ResponseEnriched", correlationId, "apiVersion", API_VERSION);
    }
}
