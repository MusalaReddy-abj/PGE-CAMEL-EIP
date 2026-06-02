package com.pge.krakencis.models.profilereads;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * One row from a Profile Reads CSV file.
 *
 * CSV column order: Verb, Noun, mRID, ReadingType, timeStamp, value
 *
 * Example row:
 *   CREATED,MeterReadings,AW8003090,0.0.0.4.1.1.12.0.0.0.0.0.0.0.0.3.72.0,2025-12-18T00:30:00+05:30,0.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProfileReadPayload {

    /** CIM verb — e.g. CREATED */
    private String verb;

    /** CIM noun — e.g. MeterReadings */
    private String noun;

    /** Meter identifier — e.g. AW8003090 */
    private String mRID;

    /** IEC reading-type OBIS code — e.g. 0.0.0.4.1.1.12.0.0.0.0.0.0.0.0.3.72.0 */
    private String readingType;

    /** ISO-8601 timestamp with timezone offset — e.g. 2025-12-18T00:30:00+05:30 */
    private String timeStamp;

    /** Meter reading value as string — e.g. 0.0 */
    private String value;
}
