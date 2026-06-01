package com.pge.krakencis.models.rcdc.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class RcdcHesResponseMessage {

    @JsonProperty("Header")  private RcdcHesResponseHeader header;
    @JsonProperty("Reply")   private RcdcHesReply          reply;
    @JsonProperty("Payload") private RcdcHesPayload        payload;
}
