package com.pge.krakencis.models.rcdc.request.target;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.Builder;
import lombok.Data;

import static com.pge.krakencis.models.rcdc.request.RcdcNamespace.IEC_NS;

@Data
@Builder
@XmlAccessorType(XmlAccessType.FIELD)
public class RcdcTargetHeader {
    @XmlElement(name = "verb",          namespace = IEC_NS) private String verb;
    @XmlElement(name = "noun",          namespace = IEC_NS) private String noun;
    @XmlElement(name = "timestamp",     namespace = IEC_NS) private String timestamp;
    @XmlElement(name = "source",        namespace = IEC_NS) private String source;
    @XmlElement(name = "replyAddress",  namespace = IEC_NS) private String replyAddress;
    @XmlElement(name = "correlationID", namespace = IEC_NS) private String correlationID;
}
