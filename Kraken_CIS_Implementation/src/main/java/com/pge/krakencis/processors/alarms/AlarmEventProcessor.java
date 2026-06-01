package com.pge.krakencis.processors.alarms;

import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.mappers.alarms.AlarmEventMapper;
import com.pge.krakencis.models.alarms.AlarmEvent;
import com.pge.krakencis.models.alarms.AlarmRequest;
import com.pge.krakencis.models.KrakenEvent;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Validates and maps an inbound {@link com.pge.krakencis.models.alarms.AlarmRequest}
 * to a list of {@link com.pge.krakencis.models.KrakenEvent} objects ready for Kafka
 * publishing.
 *
 * <p>Every event in the payload is individually validated for the presence of
 * {@code deviceIdentifierNumber}, {@code category}, and {@code createdDateTime}.
 * A missing value on any event throws a
 * {@link com.pge.krakencis.exceptions.ValidationException} (HTTP 400).
 *
 * <p>On success, the exchange body is replaced with the mapped
 * {@code List<KrakenEvent>} for downstream serialisation and Kafka publish.
 */
@Component
public class AlarmEventProcessor extends BaseProcessor {

    private static final StructuredLogger structLog = StructuredLogger.of(AlarmEventProcessor.class);

    private final AlarmEventMapper alarmEventMapper;

    public AlarmEventProcessor(AlarmEventMapper alarmEventMapper) {
        this.alarmEventMapper = alarmEventMapper;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        AlarmRequest request = exchange.getIn().getBody(AlarmRequest.class);

        validate(request, correlationId);

        List<AlarmEvent> events = request.getPayload().getEvents();
        structLog.info("alarmEventsReceived", correlationId,
            "verb",       request.getHeader().getVerb(),
            "noun",       request.getHeader().getNoun(),
            "eventCount", events.size());

        List<KrakenEvent> krakenEvents = alarmEventMapper.toKrakenEvents(events, correlationId);
        structLog.info("alarmEventsMapped", correlationId, "mappedCount", krakenEvents.size());
        exchange.getIn().setBody(krakenEvents);
    }

    private void validate(AlarmRequest request, String correlationId) {
        if (request == null)
            throw ValidationException.missingField("request body", correlationId);
        if (request.getHeader() == null)
            throw ValidationException.missingField("header", correlationId);
        if (request.getPayload() == null)
            throw ValidationException.missingField("payload", correlationId);
        if (request.getPayload().getEvents() == null || request.getPayload().getEvents().isEmpty())
            throw ValidationException.missingField("payload.events", correlationId);
        request.getPayload().getEvents().forEach(e -> validateEvent(e, correlationId));
    }

    private void validateEvent(AlarmEvent event, String correlationId) {
        if (event.getDeviceIdentifierNumber() == null || event.getDeviceIdentifierNumber().isBlank())
            throw ValidationException.missingField("deviceIdentifierNumber", correlationId);
        if (event.getCategory() == null || event.getCategory().isBlank())
            throw ValidationException.missingField("category", correlationId);
        if (event.getCreatedDateTime() == null || event.getCreatedDateTime().isBlank())
            throw ValidationException.missingField("createdDateTime", correlationId);
    }
}
