package com.pge.krakencis.models.profilereads;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.ALWAYS)
public class ProfileReadReading {

    /** Hardcoded constant: "1.0.0" */
    private String qualityType;

    /** From CSV column: value (parsed as Double) */
    private Double value;

    /** From CSV column: timeStamp */
    private String timestamp;
}
