package com.pge.krakencis.models.rcdc.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class RcdcHesResponseHeader {

    @JsonProperty("Verb")          private String verb;
    @JsonProperty("Noun")          private String noun;
    @JsonProperty("Timestamp")     private String timestamp;
    @JsonProperty("CorrelationID") private String correlationID;
}
