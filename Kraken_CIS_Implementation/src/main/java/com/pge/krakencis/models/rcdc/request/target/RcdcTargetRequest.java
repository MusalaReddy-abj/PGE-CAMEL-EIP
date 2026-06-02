package com.pge.krakencis.models.rcdc.request.target;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.Builder;
import lombok.Data;

import static com.pge.krakencis.models.rcdc.request.RcdcNamespace.IEC_NS;

@Data
@Builder
@XmlRootElement(name = "requestMessage", namespace = IEC_NS)
@XmlAccessorType(XmlAccessType.FIELD)
public class RcdcTargetRequest {
    @XmlElement(name = "header",  namespace = IEC_NS) private RcdcTargetHeader  header;
    @XmlElement(name = "payLoad", namespace = IEC_NS) private RcdcTargetPayload payload;
}
