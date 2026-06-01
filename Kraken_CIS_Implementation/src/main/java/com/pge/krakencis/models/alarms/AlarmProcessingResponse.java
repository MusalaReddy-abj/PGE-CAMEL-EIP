package com.pge.krakencis.models.alarms;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlarmProcessingResponse {
    private String status;
    private String correlationId;
    private int    eventsPublished;
    private String topic;
}
