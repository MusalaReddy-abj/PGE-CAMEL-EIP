package com.pge.krakencis.models.profilereads;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.ALWAYS)
public class ProfileReadRegister {

    /** From CSV column: ReadingType — e.g. 0.0.0.4.1.1.12.0.0.0.0.0.0.0.0.3.72.0 */
    private String readingType;

    /** Hardcoded: null */
    private String uom;

    /** Hardcoded: null */
    private String sqi;

    /** Hardcoded: null */
    private String tou;

    /** All readings (value + timestamp) for this ReadingType under one mRID */
    private List<ProfileReadReading> readings;
}
