package com.pge.krakencis.models.odr.request;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.Data;

import static com.pge.krakencis.models.odr.request.OdrNamespace.IEC_NS;

@Data
@XmlAccessorType(XmlAccessType.FIELD)
public class OdrDateRange {
    @XmlElement(name = "startTime", namespace = IEC_NS) private String startTime;
    @XmlElement(name = "endTime",   namespace = IEC_NS) private String endTime;
}
