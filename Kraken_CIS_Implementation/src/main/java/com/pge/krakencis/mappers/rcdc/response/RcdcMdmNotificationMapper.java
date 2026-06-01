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

    private static final String NAMESPACES =
        "xmlns=\"http://www.iec.ch/TC57/2008/schema/message\" " +
        "xmlns:ns2=\"http://iec.ch/TC57/2009/EndDeviceAssets#\" " +
        "xmlns:ns3=\"http://www.trilliantinc.com/SEAL/1.0/dt025pvvnl\"";

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
            + "<RequestMessage " + NAMESPACES + ">\n"
            + "  <Header>\n"
            + "    <Verb>CHANGE</Verb>\n"
            + "    <Noun>RCDSwitchState</Noun>\n"
            + "    <Timestamp>" + timestamp + "</Timestamp>\n"
            + "    <Source>" + SOURCE + "</Source>\n"
            + "    <CorrelationID>" + escapeXml(correlationId) + "</CorrelationID>\n"
            + "  </Header>\n"
            + "  <Reply>\n"
            + "    <ReplyCode>" + escapeXml(replyCode) + "</ReplyCode>\n"
            + "  </Reply>\n"
            + "  <Payload>\n"
            + "    <ns3:RCDSwitchState>\n"
            + "      <ns3:EndDeviceAsset>\n"
            + "        <ns2:mRID>" + escapeXml(mRID) + "</ns2:mRID>\n"
            + "      </ns3:EndDeviceAsset>\n"
            + "    </ns3:RCDSwitchState>\n"
            + "  </Payload>\n"
            + "</RequestMessage>";
    }

    private String escapeXml(String v) {
        if (v == null) return "";
        return v.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
                .replace("\"","&quot;").replace("'","&apos;");
    }
}
