package com.pge.krakencis.processors.rcdc.request;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.rcdc.request.RcdcAcknowledgement;
import com.pge.krakencis.processors.BaseProcessor;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.Marshaller;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.io.StringWriter;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class RcdcAcknowledgementProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(RcdcAcknowledgementProcessor.class);

    private static final JAXBContext JAXB_CTX;

    static {
        try { JAXB_CTX = JAXBContext.newInstance(RcdcAcknowledgement.class); }
        catch (Exception e) { throw new ExceptionInInitializerError(e); }
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String instanceId    = String.valueOf(
            ThreadLocalRandom.current().nextLong(1_000_000_000L, 9_999_999_999L));

        RcdcAcknowledgement ack = RcdcAcknowledgement.builder()
            .instanceID(instanceId)
            .reply(RcdcAcknowledgement.RcdcReply.builder().replyCode("0.0").build())
            .build();

        exchange.getIn().setBody(marshalToXml(ack));
        exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 200);
        exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/xml");
        exchange.getIn().setHeader("X-Correlation-ID", correlationId);

        log.info("rcdcAckSent", correlationId, "instanceID", instanceId);
    }

    private String marshalToXml(RcdcAcknowledgement ack) throws Exception {
        Marshaller m = JAXB_CTX.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        StringWriter w = new StringWriter();
        m.marshal(ack, w);
        return w.toString();
    }
}
