package com.pge.krakencis.mappers.alarms;

import com.pge.krakencis.exceptions.TransformationException;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.alarms.AlarmEvent;
import com.pge.krakencis.models.KrakenEvent;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class AlarmEventMapper {

    private static final StructuredLogger  log      = StructuredLogger.of(AlarmEventMapper.class);
    private static final String            HES_CODE = "TRILLIANT";
    private static final DateTimeFormatter DATE_ONLY = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public List<KrakenEvent> toKrakenEvents(List<AlarmEvent> events, String correlationId) {
        log.debug("mappingAlarmEvents", correlationId, "inputCount", events.size());
        List<KrakenEvent> mapped = events.stream()
            .map(e -> toKrakenEvent(e, correlationId))
            .collect(Collectors.toList());
        log.debug("alarmEventsMapped", correlationId, "outputCount", mapped.size());
        return mapped;
    }

    public KrakenEvent toKrakenEvent(AlarmEvent event, String correlationId) {
        KrakenEvent krakenEvent = KrakenEvent.builder()
            .messageId(UUID.randomUUID().toString())
            .hesCode(HES_CODE)
            .extEventName(event.getCategory())
            .externalId(event.getDeviceIdentifierNumber())
            .eventDate(parseEventDate(event.getCreatedDateTime(), correlationId))
            .build();

        log.debug("alarmEventMapped", correlationId,
            "deviceId",  event.getDeviceIdentifierNumber(),
            "messageId", krakenEvent.getMessageId(),
            "eventDate", krakenEvent.getEventDate());

        return krakenEvent;
    }

    private String parseEventDate(String createdDateTime, String correlationId) {
        try {
            return OffsetDateTime.parse(createdDateTime).format(DATE_ONLY);
        } catch (DateTimeParseException e) {
            log.error("dateParsingFailed", correlationId, e,
                "rawValue", createdDateTime,
                "expected", "ISO-8601 with offset (e.g. 2026-04-09T13:15:01.708+05:30)");
            throw TransformationException.mappingFailed(
                "createdDateTime", "eventDate (yyyy-MM-dd)", correlationId, e);
        }
    }
}
