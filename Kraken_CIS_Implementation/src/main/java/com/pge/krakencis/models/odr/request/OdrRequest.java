package com.pge.krakencis.models.odr.request;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.Data;

import static com.pge.krakencis.models.odr.request.OdrNamespace.IEC_NS;

@Data
@XmlRootElement(name = "requestMessage", namespace = IEC_NS)
@XmlAccessorType(XmlAccessType.FIELD)
public class OdrRequest {
    @XmlElement(name = "header",  namespace = IEC_NS) private OdrHeader  header;
    @XmlElement(name = "payLoad", namespace = IEC_NS) private OdrPayload payload;
}
