package com.pge.krakencis.mappers.rcdc.response;

import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.rcdc.response.RcdcHesResponseMessage;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class RcdcMdmNotificationMapper {

    private static final StructuredLogger  log      = StructuredLogger.of(RcdcMdmNotificationMapper.class);
    private static final String            SOURCE   = "KRAKEN-CIS";
    private static final DateTimeFormatter ISO_DT   = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private static final String SOAP_NS  = "http://schemas.xmlsoap.org/soap/envelope/";
    private static final String MSG_NS   = "http://www.iec.ch/TC57/2008/schema/message";
    private static final String ASSET_NS = "http://iec.ch/TC57/2009/EndDeviceAssets#";
    private static final String SEAL_NS  = "http://www.trilliantinc.com/SEAL/1.0/dt025pvvnl";

    public String toSoapXml(RcdcHesResponseMessage response, String correlationId) {
        String mRID      = response.getPayload().getDefaultResponse()
                                    .getEndDeviceAsset().getMRID();
        String replyCode = response.getReply().getReplyCode();
        String timestamp = OffsetDateTime.now().format(ISO_DT);

        log.info("rcdcMdmNotificationMapped", correlationId, "mRID", mRID, "replyCode", replyCode);
        return buildXml(correlationId, mRID, replyCode, timestamp);
    }

    private String buildXml(String correlationId, String mRID, String replyCode, String timestamp) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<soapenv:Envelope"
            + " xmlns:soapenv=\"" + SOAP_NS  + "\""
            + " xmlns:msg=\""     + MSG_NS   + "\""
            + " xmlns:ns2=\""     + ASSET_NS + "\""
            + " xmlns:ns3=\""     + SEAL_NS  + "\""
            + ">\n"
            + "  <soapenv:Header/>\n"
            + "  <soapenv:Body>\n"
            + "    <msg:RequestMessage>\n"
            + "      <msg:Header>\n"
            + "        <msg:Verb>CHANGE</msg:Verb>\n"
            + "        <msg:Noun>RCDSwitchState</msg:Noun>\n"
            + "        <msg:Timestamp>" + timestamp + "</msg:Timestamp>\n"
            + "        <msg:Source>" + SOURCE + "</msg:Source>\n"
            + "        <msg:CorrelationID>" + escapeXml(correlationId) + "</msg:CorrelationID>\n"
            + "      </msg:Header>\n"
            + "      <msg:Reply>\n"
            + "        <msg:ReplyCode>" + escapeXml(replyCode) + "</msg:ReplyCode>\n"
            + "      </msg:Reply>\n"
            + "      <msg:Payload>\n"
            + "        <ns3:RCDSwitchState>\n"
            + "          <ns3:EndDeviceAsset>\n"
            + "            <ns2:mRID>" + escapeXml(mRID) + "</ns2:mRID>\n"
            + "          </ns3:EndDeviceAsset>\n"
            + "        </ns3:RCDSwitchState>\n"
            + "      </msg:Payload>\n"
            + "    </msg:RequestMessage>\n"
            + "  </soapenv:Body>\n"
            + "</soapenv:Envelope>";
    }

    private String escapeXml(String v) {
        if (v == null) return "";
        return v.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
                .replace("\"","&quot;").replace("'","&apos;");
    }
}
