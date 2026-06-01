package com.pge.krakencis.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KrakenEvent {
    private String messageId;
    private String hesCode;
    private String extEventName;
    private String externalId;
    private String eventDate;
}
