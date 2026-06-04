package com.pge.krakencis.mappers.rcdc.response;

import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.rcdc.response.RcdcHesResponseMessage;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RcdcMdmNotificationMapper {

    private static final StructuredLogger  log    = StructuredLogger.of(RcdcMdmNotificationMapper.class);
    private static final DateTimeFormatter ISO_DT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    // Response header constants for the CM-ChangeRCDSwitchStateResp envelope.
    private static final String VERB = "REPLY";
    private static final String NOUN = "DefaultResponse";

    // SOAP / Oracle OUAF / Trilliant namespaces for the response envelope.
    private static final String SOAP_NS  = "http://schemas.xmlsoap.org/soap/envelope/";
    private static final String RESP_NS  = "http://ouaf.oracle.com/webservices/cm/CM-ChangeRCDSwitchStateResp";
    private static final String JCA_NS   = "http://xmlns.oracle.com/pcbpel/wsdl/jca/";
    private static final String ASSET_NS = "http://iec.ch/TC57/2009/EndDeviceAssets#";
    private static final String NS1_NS   = "http://xmlns.oracle.com/pcbpel/adapter/jms/TrilliantSOAApplication/TrilliantRCDCAsyncRespFromHESToMDM/ConsumeResponseFromTrilliant";
    private static final String BASIC_NS = "http://www.trilliantinc.com/SEAL/1.0/BasicTypes";
    private static final String SEAL_NS  = "http://www.trilliantinc.com/SEAL/1.0/dt025pvvnl";

    public String toSoapXml(RcdcHesResponseMessage response, String correlationId) {
        String replyCode = response.getReply().getReplyCode();
        String timestamp = OffsetDateTime.now(ZoneOffset.UTC).format(ISO_DT);

        log.info("rcdcMdmNotificationMapped", correlationId, "replyCode", replyCode);
        return buildXml(correlationId, replyCode, timestamp);
    }

    private String buildXml(String correlationId, String replyCode, String timestamp) {
        String cid = escapeXml(correlationId);
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<soapenv:Envelope xmlns:soapenv=\"" + SOAP_NS + "\">\n"
            + "  <soapenv:Header/>\n"
            + "  <soapenv:Body>\n"
            + "    <CM-ChangeRCDSwitchStateResp"
            + " xmlns=\""      + RESP_NS  + "\""
            + " xmlns:jca=\""  + JCA_NS   + "\""
            + " xmlns:ns2=\""  + ASSET_NS + "\""
            + " xmlns:ns1=\""  + NS1_NS   + "\""
            + " xmlns:ns4=\""  + BASIC_NS + "\""
            + " xmlns:ns3=\""  + SEAL_NS  + "\""
            + " xmlns:tns=\""  + RESP_NS  + "\""
            + ">\n"
            + "      <tns:transactionId>" + cid + "</tns:transactionId>\n"
            + "      <tns:responseDetail>\n"
            + "        <tns:header>\n"
            + "          <tns:verb>" + VERB + "</tns:verb>\n"
            + "          <tns:noun>" + NOUN + "</tns:noun>\n"
            + "          <tns:correlationID>" + cid + "</tns:correlationID>\n"
            + "          <tns:timeStamp>" + timestamp + "</tns:timeStamp>\n"
            + "        </tns:header>\n"
            + "        <tns:reply>\n"
            + "          <tns:replyCode>" + escapeXml(replyCode) + "</tns:replyCode>\n"
            + "        </tns:reply>\n"
            + "        <tns:payLoad/>\n"
            + "      </tns:responseDetail>\n"
            + "    </CM-ChangeRCDSwitchStateResp>\n"
            + "  </soapenv:Body>\n"
            + "</soapenv:Envelope>";
    }

    private String escapeXml(String v) {
        if (v == null) return "";
        return v.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
                .replace("\"","&quot;").replace("'","&apos;");
    }
}
