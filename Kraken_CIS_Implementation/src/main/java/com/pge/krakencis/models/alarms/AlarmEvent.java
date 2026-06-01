package com.pge.krakencis.models.alarms;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlarmEvent {
    private String category;
    private String createdDateTime;
    private String description;
    private String deviceIdentifierNumber;
    private String reason;
    private String value;
}
