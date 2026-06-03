package com.pge.krakencis.models.odr.request;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.Data;

import static com.pge.krakencis.models.odr.request.OdrNamespace.IEC_NS;

@Data
@XmlAccessorType(XmlAccessType.FIELD)
public class OdrHeader {
    @XmlElement(name = "verb",          namespace = IEC_NS) private String verb;
    @XmlElement(name = "noun",          namespace = IEC_NS) private String noun;
    @XmlElement(name = "replyAddress",  namespace = IEC_NS) private String replyAddress;
    @XmlElement(name = "correlationID", namespace = IEC_NS) private String correlationID;
    @XmlElement(name = "timeStamp",     namespace = IEC_NS) private String timeStamp;
}
