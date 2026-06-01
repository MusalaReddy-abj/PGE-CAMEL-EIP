package com.pge.krakencis.models.rcdc.request;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import static com.pge.krakencis.models.rcdc.request.RcdcNamespace.IEC_NS;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@XmlRootElement(name = "responseDetail", namespace = IEC_NS)
@XmlAccessorType(XmlAccessType.FIELD)
public class RcdcAcknowledgement {

    @XmlElement(name = "instanceID") private String     instanceID;
    @XmlElement(name = "reply")      private RcdcReply  reply;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class RcdcReply {
        @XmlElement(name = "replyCode")    private String replyCode;
        @XmlElement(name = "errorMessage") private String errorMessage;
    }
}
