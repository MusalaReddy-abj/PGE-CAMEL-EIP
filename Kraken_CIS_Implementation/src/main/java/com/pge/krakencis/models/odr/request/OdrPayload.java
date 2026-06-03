package com.pge.krakencis.models.odr.request;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.Data;

import static com.pge.krakencis.models.odr.request.OdrNamespace.IEC_NS;

@Data
@XmlAccessorType(XmlAccessType.FIELD)
public class OdrPayload {
    @XmlElement(name = "meterReadings", namespace = IEC_NS) private OdrMeterReadings meterReadings;
}
