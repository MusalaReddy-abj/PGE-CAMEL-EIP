package com.pge.krakencis.processors;

import com.pge.krakencis.logging.StructuredLogger;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

/**
 * Wraps the exchange body (an XML string) in a SOAP 1.1 envelope before it is
 * returned to the caller.
 *
 * <p>Any existing {@code <?xml ...?>} processing instruction is stripped from
 * the inner XML and re-emitted only at the envelope level, keeping the document
 * well-formed.
 *
 * <p>Content-Type is set to {@code text/xml; charset=utf-8} which is the
 * correct media type for SOAP 1.1 messages.
 */
@Component
public class SoapEnvelopeWrapperProcessor extends BaseProcessor {

    private static final StructuredLogger log     = StructuredLogger.of(SoapEnvelopeWrapperProcessor.class);
    private static final String           SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/";

    @Override
    public void process(Exchange exchange) throws Exception {
        String body = exchange.getIn().getBody(String.class);
        if (body == null || body.isBlank()) {
            return;
        }

        // Strip inner XML declaration so only the envelope-level one is present
        String innerXml = body.replaceFirst("(?s)^\\s*<\\?xml[^?]*\\?>\\s*", "").strip();

        String wrapped = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<soapenv:Envelope xmlns:soapenv=\"" + SOAP_NS + "\">\n"
            + "  <soapenv:Header/>\n"
            + "  <soapenv:Body>\n"
            + "    " + innerXml + "\n"
            + "  </soapenv:Body>\n"
            + "</soapenv:Envelope>";

        exchange.getIn().setBody(wrapped);
        exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "text/xml; charset=utf-8");

        log.info("soapEnvelopeWrapped", null);
    }
}
