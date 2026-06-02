package com.pge.krakencis.mappers.profilereads;

import com.pge.krakencis.exceptions.TransformationException;
import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.profilereads.ProfileReadPayload;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Maps a Profile Reads CSV file into {@link ProfileReadPayload} objects.
 *
 * <h3>Expected CSV format</h3>
 * <pre>
 * Verb,Noun,mRID,ReadingType,timeStamp,value
 * CREATED,MeterReadings,AW8003090,0.0.0.4.1.1.12.0.0.0.0.0.0.0.0.3.72.0,2025-12-18T00:30:00+05:30,0.0
 * </pre>
 *
 * <ul>
 *   <li>The first row is always the header — it is skipped by the processor.</li>
 *   <li>Each subsequent row represents one meter reading interval.</li>
 *   <li>Column lookup is by name (case-insensitive) so column order changes are tolerated.</li>
 * </ul>
 */
@Component
public class ProfileReadsMapper {

    private static final StructuredLogger log = StructuredLogger.of(ProfileReadsMapper.class);

    // Exact header names as they appear in the CSV (stored in lower-case for lookup)
    private static final String COL_VERB        = "verb";
    private static final String COL_NOUN        = "noun";
    private static final String COL_MRID        = "mrid";
    private static final String COL_READING_TYPE = "readingtype";
    private static final String COL_TIMESTAMP   = "timestamp";
    private static final String COL_VALUE       = "value";

    private static final String[] REQUIRED_HEADERS = {
        COL_VERB, COL_NOUN, COL_MRID, COL_READING_TYPE, COL_TIMESTAMP, COL_VALUE
    };

    // ── Public API used by ProfileReadsCsvProcessor ───────────────────────────

    /**
     * Builds a column-name → column-index map from the CSV header line.
     * Header names are normalised to lower-case so lookup is case-insensitive.
     */
    public Map<String, Integer> buildHeaderIndex(String headerLine) {
        String[] headers = splitCsvLine(headerLine);
        Map<String, Integer> headerIndex = new LinkedHashMap<>();
        for (int i = 0; i < headers.length; i++) {
            headerIndex.put(headers[i].trim().toLowerCase(Locale.ROOT), i);
        }
        return headerIndex;
    }

    /**
     * Validates that all required columns are present in the header.
     *
     * @throws ValidationException listing every missing column name
     */
    public void validateHeaders(Map<String, Integer> headerIndex, String correlationId) {
        List<String> missing = new ArrayList<>();
        for (String required : REQUIRED_HEADERS) {
            if (!headerIndex.containsKey(required)) {
                missing.add(required);
            }
        }
        if (!missing.isEmpty()) {
            throw ValidationException.invalidFormat(
                "CSV header",
                "missing required column(s): " + String.join(", ", missing),
                correlationId);
        }
    }

    /**
     * Parses one data row into a {@link ProfileReadPayload}.
     *
     * @throws TransformationException if a required field is absent or empty
     */
    public ProfileReadPayload parseRow(String line, Map<String, Integer> headerIndex,
                                       String correlationId) {
        String[] fields = splitCsvLine(line);

        String verb        = requireField(fields, headerIndex, COL_VERB,         correlationId);
        String noun        = requireField(fields, headerIndex, COL_NOUN,         correlationId);
        String mRID        = requireField(fields, headerIndex, COL_MRID,         correlationId);
        String readingType = requireField(fields, headerIndex, COL_READING_TYPE, correlationId);
        String timeStamp   = requireField(fields, headerIndex, COL_TIMESTAMP,    correlationId);
        String value       = getField(fields, headerIndex, COL_VALUE);  // value may be 0.0 or empty

        log.debug("profileReadRowParsed", correlationId,
            "mRID",        mRID,
            "readingType", readingType,
            "timeStamp",   timeStamp,
            "value",       value);

        return ProfileReadPayload.builder()
            .verb(verb)
            .noun(noun)
            .mRID(mRID)
            .readingType(readingType)
            .timeStamp(timeStamp)
            .value(value)
            .build();
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /**
     * Splits a CSV line by comma.
     * Handles trailing commas (empty last field) via the {@code -1} limit.
     */
    private String[] splitCsvLine(String line) {
        return line.split(",", -1);
    }

    /**
     * Returns the trimmed field value for {@code column}, throwing
     * {@link TransformationException} if the column is missing or the value is blank.
     */
    private String requireField(String[] fields, Map<String, Integer> headerIndex,
                                 String column, String correlationId) {
        String value = getField(fields, headerIndex, column);
        if (value == null || value.isBlank()) {
            throw TransformationException.mappingFailed(column, "CSV value", correlationId, null);
        }
        return value;
    }

    /**
     * Returns the trimmed field value for {@code column}, or {@code null} if the
     * column index is beyond the available fields (tolerates short rows).
     */
    private String getField(String[] fields, Map<String, Integer> headerIndex, String column) {
        Integer index = headerIndex.get(column);
        if (index == null || index >= fields.length) {
            return null;
        }
        return fields[index].trim();
    }
}
