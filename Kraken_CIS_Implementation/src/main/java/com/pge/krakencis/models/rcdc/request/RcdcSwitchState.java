package com.pge.krakencis.models.rcdc.request;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.Data;

import static com.pge.krakencis.models.rcdc.request.RcdcNamespace.IEC_NS;

@Data
@XmlAccessorType(XmlAccessType.FIELD)
public class RcdcSwitchState {
    @XmlElement(name = "endDeviceAsset", namespace = IEC_NS) private RcdcEndDeviceAsset endDeviceAsset;
    @XmlElement(name = "state",          namespace = IEC_NS) private String             state;
}
