package com.pge.krakencis.models.rcdc.request;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.Data;

import static com.pge.krakencis.models.rcdc.request.RcdcNamespace.IEC_NS;

@Data
@XmlAccessorType(XmlAccessType.FIELD)
public class RcdcPayload {
    @XmlElement(name = "RCDSwitchState", namespace = IEC_NS) private RcdcSwitchState rcdSwitchState;
}
