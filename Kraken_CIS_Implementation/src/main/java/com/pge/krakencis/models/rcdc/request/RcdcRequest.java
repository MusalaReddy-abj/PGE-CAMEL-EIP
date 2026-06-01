package com.pge.krakencis.models.rcdc.request;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.Data;

import static com.pge.krakencis.models.rcdc.request.RcdcNamespace.IEC_NS;

@Data
@XmlRootElement(name = "requestMessage", namespace = IEC_NS)
@XmlAccessorType(XmlAccessType.FIELD)
public class RcdcRequest {
    @XmlElement(name = "header",  namespace = IEC_NS) private RcdcHeader  header;
    @XmlElement(name = "payLoad", namespace = IEC_NS) private RcdcPayload payload;
}
