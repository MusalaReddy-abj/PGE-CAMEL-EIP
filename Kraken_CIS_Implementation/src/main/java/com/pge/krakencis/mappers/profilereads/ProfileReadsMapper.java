package com.pge.krakencis.mappers.profilereads;

import com.pge.krakencis.exceptions.TransformationException;
import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.profilereads.KafkaProfileReadPayload;
import com.pge.krakencis.models.profilereads.ProfileReadPayload;
import com.pge.krakencis.models.profilereads.ProfileReadReading;
import com.pge.krakencis.models.profilereads.ProfileReadRegister;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Maps a Profile Reads CSV file into {@link KafkaProfileReadPayload} objects
 * ready for publishing to the Kafka profile-reads topic.
 *
 * <h3>Pipeline</h3>
 * <ol>
 *   <li>{@link #buildHeaderIndex} — parses the CSV header line into a column-name → index map.</li>
 *   <li>{@link #validateHeaders} — asserts all required columns are present.</li>
 *   <li>{@link #parseRow}       — converts one CSV data row into a raw {@link ProfileReadPayload}.</li>
 *   <li>{@link #toKafkaMessages}— groups all raw rows by mRID → ReadingType → produces one
 *       {@link KafkaProfileReadPayload} per mRID with nested registers and readings.</li>
 * </ol>
 *
 * <h3>Expected CSV format</h3>
 * <pre>
 * Verb,Noun,mRID,ReadingType,timeStamp,value
 * CREATED,MeterReadings,AW8003090,0.0.0.4.1.1.12.0.0.0.0.0.0.0.0.3.72.0,2025-12-18T00:30:00+05:30,0.0
 * </pre>
 *
 * <h3>Kafka payload structure</h3>
 * <pre>
 * {
 *   "externalId":  "AW8003090",        // mRID
 *   "sourceHes":   "SENSUS",           // constant
 *   "profileName": "INTERVAL_READS",   // constant
 *   "registers": [{
 *     "readingType": "0.0.0.4...",      // ReadingType
 *     "uom": null, "sqi": null, "tou": null,
 *     "readings": [
 *       { "qualityType": "1.0.0", "value": 0.0, "timestamp": "2025-12-18T00:30:00+05:30" }
 *     ]
 *   }]
 * }
 * </pre>
 */
@Component
public class ProfileReadsMapper {

    private static final StructuredLogger log = StructuredLogger.of(ProfileReadsMapper.class);

    // ── CSV column keys (lower-cased for case-insensitive lookup) ─────────────
    private static final String COL_VERB         = "verb";
    private static final String COL_NOUN         = "noun";
    private static final String COL_MRID         = "mrid";
    private static final String COL_READING_TYPE = "readingtype";
    private static final String COL_TIMESTAMP    = "timestamp";
    private static final String COL_VALUE        = "value";

    private static final String[] REQUIRED_HEADERS = {
        COL_VERB, COL_NOUN, COL_MRID, COL_READING_TYPE, COL_TIMESTAMP, COL_VALUE
    };

    // ── Hardcoded constants for the Kafka payload ─────────────────────────────
    private static final String SOURCE_HES    = "TRILLIANT";
    private static final String PROFILE_NAME  = "INTERVAL_READS";
    private static final String QUALITY_TYPE  = "1.0.0";

    // ── Step 1: build column-index map from header line ───────────────────────

    public Map<String, Integer> buildHeaderIndex(String headerLine) {
        String[] headers = splitCsvLine(headerLine);
        Map<String, Integer> idx = new LinkedHashMap<>();
        for (int i = 0; i < headers.length; i++) {
            idx.put(headers[i].trim().toLowerCase(Locale.ROOT), i);
        }
        return idx;
    }

    // ── Step 2: validate all required columns are present ────────────────────

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

    // ── Step 3: parse one CSV data row → raw ProfileReadPayload ──────────────

    public ProfileReadPayload parseRow(String line, Map<String, Integer> headerIndex,
                                       String correlationId) {
        String[] fields = splitCsvLine(line);

        String verb        = requireField(fields, headerIndex, COL_VERB,         correlationId);
        String noun        = requireField(fields, headerIndex, COL_NOUN,         correlationId);
        String mRID        = requireField(fields, headerIndex, COL_MRID,         correlationId);
        String readingType = requireField(fields, headerIndex, COL_READING_TYPE, correlationId);
        String timeStamp   = requireField(fields, headerIndex, COL_TIMESTAMP,    correlationId);
        String value       = getField(fields, headerIndex, COL_VALUE);

        return ProfileReadPayload.builder()
            .verb(verb)
            .noun(noun)
            .mRID(mRID)
            .readingType(readingType)
            .timeStamp(timeStamp)
            .value(value)
            .build();
    }

    // ── Step 4: group raw rows → Kafka messages ───────────────────────────────

    /**
     * Groups a flat list of raw CSV rows into Kafka messages.
     *
     * <p><b>Grouping strategy:</b>
     * <ol>
     *   <li>Group rows by {@code mRID} — one {@link KafkaProfileReadPayload} per mRID.</li>
     *   <li>Within each mRID group, sub-group by {@code ReadingType} — one
     *       {@link ProfileReadRegister} per distinct ReadingType.</li>
     *   <li>Within each register, collect all (value, timestamp) pairs as
     *       {@link ProfileReadReading} objects.</li>
     * </ol>
     *
     * @param rows          raw rows parsed from the CSV (one entry per CSV data line)
     * @param correlationId used for debug logging
     * @return one Kafka message per unique mRID found in {@code rows}
     */
    /**
     * Groups rows by (mRID + ReadingType) — one Kafka message per unique pair.
     *
     * Each message carries exactly ONE register (the ReadingType for that group)
     * with ALL readings (value + timestamp) collected from every CSV row that
     * shares that mRID and ReadingType.
     *
     * Example — two CSV rows with the same mRID and ReadingType:
     * <pre>
     * CREATED,MeterReadings,AW8003090,0.0.0.4...,2025-12-18T00:30:00+05:30,0.0
     * CREATED,MeterReadings,AW8003090,0.0.0.4...,2025-12-18T01:00:00+05:30,0.0
     * → ONE Kafka message with two readings inside one register.
     * </pre>
     */
    public List<KafkaProfileReadPayload> toKafkaMessages(List<ProfileReadPayload> rows,
                                                          String correlationId) {
        // Key: "mRID|ReadingType" → all rows in that group
        Map<String, List<ProfileReadPayload>> grouped = new LinkedHashMap<>();

        for (ProfileReadPayload row : rows) {
            String key = row.getMRID() + "|" + row.getReadingType();
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }

        List<KafkaProfileReadPayload> messages = new ArrayList<>();

        for (List<ProfileReadPayload> group : grouped.values()) {
            ProfileReadPayload first = group.get(0);

            List<ProfileReadReading> readings = group.stream()
                .map(row -> ProfileReadReading.builder()
                    .qualityType(QUALITY_TYPE)
                    .value(parseDouble(row.getValue()))
                    .timestamp(row.getTimeStamp())
                    .build())
                .collect(Collectors.toList());

            ProfileReadRegister register = ProfileReadRegister.builder()
                .readingType(first.getReadingType())
                .uom(null)
                .sqi(null)
                .tou(null)
                .readings(readings)
                .build();

            messages.add(KafkaProfileReadPayload.builder()
                .externalId(first.getMRID())
                .sourceHes(SOURCE_HES)
                .profileName(PROFILE_NAME)
                .registers(List.of(register))
                .build());
        }

        log.debug("profileReadsGrouped", correlationId,
            "inputRows",     rows.size(),
            "kafkaMessages", messages.size());

        return messages;
    }

    /**
     * Parallel version of {@link #toKafkaMessages} for large row sets.
     *
     * <p>Uses a parallel stream to group rows across multiple CPU cores.
     * Recommended when {@code rows.size() >= 2000}.  For smaller lists the
     * sequential version is faster due to fork/join overhead.
     */
    public List<KafkaProfileReadPayload> toKafkaMessagesParallel(List<ProfileReadPayload> rows,
                                                                   String correlationId) {
        // Parallel groupBy: "mRID|ReadingType" → list of rows
        Map<String, List<ProfileReadPayload>> grouped =
            rows.parallelStream().collect(
                Collectors.groupingByConcurrent(
                    row -> row.getMRID() + "|" + row.getReadingType()
                )
            );

        List<KafkaProfileReadPayload> messages = grouped.values().parallelStream()
            .map(group -> {
                ProfileReadPayload first = group.get(0);

                List<ProfileReadReading> readings = group.stream()
                    .map(row -> ProfileReadReading.builder()
                        .qualityType(QUALITY_TYPE)
                        .value(parseDouble(row.getValue()))
                        .timestamp(row.getTimeStamp())
                        .build())
                    .collect(Collectors.toList());

                return KafkaProfileReadPayload.builder()
                    .externalId(first.getMRID())
                    .sourceHes(SOURCE_HES)
                    .profileName(PROFILE_NAME)
                    .registers(List.of(ProfileReadRegister.builder()
                        .readingType(first.getReadingType())
                        .uom(null).sqi(null).tou(null)
                        .readings(readings)
                        .build()))
                    .build();
            })
            .collect(Collectors.toList());

        log.debug("profileReadsGroupedParallel", correlationId,
            "inputRows",     rows.size(),
            "kafkaMessages", messages.size());

        return messages;
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private String[] splitCsvLine(String line) {
        return line.split(",", -1);
    }

    private String requireField(String[] fields, Map<String, Integer> headerIndex,
                                 String column, String correlationId) {
        String val = getField(fields, headerIndex, column);
        if (val == null || val.isBlank()) {
            throw TransformationException.mappingFailed(column, "CSV value", correlationId, null);
        }
        return val;
    }

    private String getField(String[] fields, Map<String, Integer> headerIndex, String column) {
        Integer index = headerIndex.get(column);
        if (index == null || index >= fields.length) {
            return null;
        }
        return fields[index].trim();
    }

    private Double parseDouble(String raw) {
        if (raw == null || raw.isBlank()) return null;
        try {
            return Double.parseDouble(raw);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
