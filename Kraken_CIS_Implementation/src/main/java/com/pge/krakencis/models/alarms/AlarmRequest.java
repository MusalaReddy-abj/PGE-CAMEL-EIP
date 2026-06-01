package com.pge.krakencis.models.alarms;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlarmRequest {
    private AlarmHeader  header;
    private AlarmPayload payload;
}
