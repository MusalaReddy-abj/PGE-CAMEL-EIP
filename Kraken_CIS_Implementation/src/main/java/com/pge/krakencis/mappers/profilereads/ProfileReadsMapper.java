package com.pge.krakencis.mappers.profilereads;

import com.pge.krakencis.exceptions.TransformationException;
import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.profilereads.ProfileReadPayload;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

@Component
public class ProfileReadsMapper {

    private static final StructuredLogger log = StructuredLogger.of(ProfileReadsMapper.class);
    private static final String[] REQUIRED_HEADERS = {
        "externalid", "sourcehes", "profilename", "registers", "readings"
    };

    public List<ProfileReadPayload> toPayloads(String csvBody, String correlationId) {
        if (csvBody == null || csvBody.trim().isEmpty()) {
            throw ValidationException.missingField("FTP CSV body", correlationId);
        }

        List<ProfileReadPayload> payloads = new ArrayList<>();
        int rowCount = 0;

        try (Scanner scanner = new Scanner(csvBody)) {
            if (!scanner.hasNextLine()) {
                throw ValidationException.missingField("FTP CSV header", correlationId);
            }

            String headerLine = scanner.nextLine().trim();
            Map<String, Integer> headerIndex = buildHeaderIndex(headerLine);
            validateHeaders(headerIndex, correlationId);

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                if (!line.isEmpty()) {
                    payloads.add(parseRow(line, headerIndex, correlationId));
                    rowCount++;
                }
            }
        }

        log.debug("profileReadsCsvMapped", correlationId,
            "rowCount", rowCount,
            "totalPayloads", payloads.size());

        return payloads;
    }

    public Map<String, Integer> buildHeaderIndex(String headerLine) {
        String[] headers = parseCsvLine(headerLine);
        Map<String, Integer> headerIndex = new LinkedHashMap<>();
        for (int i = 0; i < headers.length; i++) {
            headerIndex.put(headers[i].trim().toLowerCase(Locale.ROOT), i);
        }
        return headerIndex;
    }

    public void validateHeaders(Map<String, Integer> headerIndex, String correlationId) {
        List<String> missingHeaders = new ArrayList<>();
        for (String header : REQUIRED_HEADERS) {
            if (!headerIndex.containsKey(header)) {
                missingHeaders.add(header);
            }
        }

        if (!missingHeaders.isEmpty()) {
            throw ValidationException.invalidFormat(
                "CSV header", "missing required header(s): " + String.join(", ", missingHeaders), correlationId);
        }
    }

    public ProfileReadPayload parseRow(String line, Map<String, Integer> headerIndex, String correlationId) {
        String[] fields = parseCsvLine(line);

        return ProfileReadPayload.builder()
            .externalId(getField(fields, headerIndex, "externalid", correlationId))
            .sourceHes(getField(fields, headerIndex, "sourcehes", correlationId))
            .profileName(getField(fields, headerIndex, "profilename", correlationId))
            .registers(parseList(getField(fields, headerIndex, "registers", correlationId)))
            .readings(parseList(getField(fields, headerIndex, "readings", correlationId)))
            .build();
    }

    private String getField(String[] fields, Map<String, Integer> headerIndex,
                            String headerName, String correlationId) {
        Integer index = headerIndex.get(headerName);
        if (index == null || index >= fields.length) {
            throw TransformationException.mappingFailed(headerName, "CSV value", correlationId, null);
        }
        return fields[index].trim();
    }

    private String[] parseCsvLine(String line) {
        return line.split(",", -1);
    }

    private List<String> parseList(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return List.of();
        }
        return Arrays.stream(rawValue.split("[;|]", -1))
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .collect(Collectors.toList());
    }
}
