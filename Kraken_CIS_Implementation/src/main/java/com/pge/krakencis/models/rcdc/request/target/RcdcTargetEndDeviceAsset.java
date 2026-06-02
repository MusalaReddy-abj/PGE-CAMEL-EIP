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
public class RcdcTargetEndDeviceAsset {
    @XmlElement(name = "mRID", namespace = IEC_NS) private String mRID;
}
