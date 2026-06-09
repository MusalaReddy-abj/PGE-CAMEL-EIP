package com.pge.krakencis.models.odr.request;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.Data;

import static com.pge.krakencis.models.odr.request.OdrNamespace.IEC_NS;

@Data
@XmlAccessorType(XmlAccessType.FIELD)
public class OdrMeterReadings {
    @XmlElement(name = "endDeviceAsset", namespace = IEC_NS) private OdrEndDeviceAsset endDeviceAsset;
    @XmlElement(name = "profile",        namespace = IEC_NS) private String profile;
    @XmlElement(name = "currentRead",    namespace = IEC_NS) private String currentRead;
    @XmlElement(name = "daily",          namespace = IEC_NS) private String daily;
    @XmlElement(name = "monthly",        namespace = IEC_NS) private String monthly;
    @XmlElement(name = "instantaneous",  namespace = IEC_NS) private String instantaneous;
    @XmlElement(name = "event",          namespace = IEC_NS) private String event;
    @XmlElement(name = "reading",        namespace = IEC_NS) private String reading;
    @XmlElement(name = "dateRange",      namespace = IEC_NS) private OdrDateRange dateRange;
}
