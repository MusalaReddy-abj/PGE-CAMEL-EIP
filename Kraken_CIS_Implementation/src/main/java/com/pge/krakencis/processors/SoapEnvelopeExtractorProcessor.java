package com.pge.krakencis.processors;

import com.pge.krakencis.logging.StructuredLogger;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * Strips a SOAP 1.1 or SOAP 1.2 envelope from the exchange body and replaces it
 * with the first child element found inside {@code <soapenv:Body>}.
 *
 * <p>If the body is not a SOAP envelope (plain XML) it is passed through unchanged,
 * so this processor can be placed safely on any XML route regardless of whether
 * callers always wrap in SOAP.
 *
 * <p>The original full-envelope body should be saved to an exchange property
 * <em>before</em> this processor runs when it needs to be forwarded downstream
 * (e.g. to a mock SOAP service).
 */
@Component
public class SoapEnvelopeExtractorProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(SoapEnvelopeExtractorProcessor.class);

    private static final String SOAP_11_NS = "http://schemas.xmlsoap.org/soap/envelope/";
    private static final String SOAP_12_NS = "http://www.w3.org/2003/05/soap-envelope";

    @Override
    public void process(Exchange exchange) throws Exception {
        String body = exchange.getIn().getBody(String.class);
        if (body == null || body.isBlank() || !body.contains("Envelope")) {
            return;
        }

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        // XXE prevention
        dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
        dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        dbf.setXIncludeAware(false);
        dbf.setExpandEntityReferences(false);

        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new InputSource(new StringReader(body)));
        Element root = doc.getDocumentElement();

        String ns = root.getNamespaceURI();
        if (!SOAP_11_NS.equals(ns) && !SOAP_12_NS.equals(ns)) {
            return;
        }

        NodeList bodyNodes = root.getElementsByTagNameNS(ns, "Body");
        if (bodyNodes.getLength() == 0) {
            log.warn("soapEnvelopeExtract_noBodyFound", null);
            return;
        }

        Element soapBody = (Element) bodyNodes.item(0);
        Element innerElement = firstChildElement(soapBody);
        if (innerElement == null) {
            log.warn("soapEnvelopeExtract_emptyBody", null);
            return;
        }

        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        StringWriter sw = new StringWriter();
        transformer.transform(new DOMSource(innerElement), new StreamResult(sw));

        exchange.getIn().setBody(sw.toString());

        log.info("soapEnvelopeExtracted", null,
            "bodyElement", innerElement.getLocalName(),
            "namespace",   innerElement.getNamespaceURI());
    }

    private static Element firstChildElement(Element parent) {
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node instanceof Element) {
                return (Element) node;
            }
        }
        return null;
    }
}
