package com.pge.krakencis.models.profilereads;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Kafka message payload for the profile-reads topic.
 *
 * One message is published per unique mRID found in the CSV file.
 * Multiple ReadingType values for the same mRID become separate entries
 * in the {@code registers} list.
 *
 * Example:
 * <pre>
 * {
 *   "externalId":  "AW8003090",       // from CSV: mRID
 *   "sourceHes":   "TRILLIANT",          // constant
 *   "profileName": "INTERVAL_READS",  // constant
 *   "registers": [
 *     {
 *       "readingType": "0.0.0.4.1.1.12.0.0.0.0.0.0.0.0.3.72.0",  // from CSV: ReadingType
 *       "uom": null, "sqi": null, "tou": null,                     // constants
 *       "readings": [
 *         { "qualityType": "1.0.0", "value": 0.0, "timestamp": "2025-12-18T00:30:00+05:30" },
 *         { "qualityType": "1.0.0", "value": 0.0, "timestamp": "2025-12-18T01:00:00+05:30" }
 *       ]
 *     }
 *   ]
 * }
 * </pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.ALWAYS)
public class KafkaProfileReadPayload {

    /** From CSV column: mRID */
    private String externalId;

    /** Hardcoded constant: "TRILLIANT" */
    private String sourceHes;

    /** Hardcoded constant: "INTERVAL_READS" */
    private String profileName;

    /** One entry per unique ReadingType for this mRID */
    private List<ProfileReadRegister> registers;
}
