package com.pge.krakencis.processors.profilereads;

import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.mappers.profilereads.ProfileReadsMapper;
import com.pge.krakencis.models.profilereads.ProfileReadPayload;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ProfileReadsCsvProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(ProfileReadsCsvProcessor.class);

    private final ProfileReadsMapper profileReadsMapper;

    public ProfileReadsCsvProcessor(ProfileReadsMapper profileReadsMapper) {
        this.profileReadsMapper = profileReadsMapper;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String fileName      = exchange.getIn().getHeader("CamelFileName", String.class);
        String csvBody       = exchange.getIn().getBody(String.class);

        if (csvBody == null || csvBody.isBlank()) {
            throw ValidationException.missingField("FTP CSV body", correlationId);
        }

        List<ProfileReadPayload> payloads = profileReadsMapper.toPayloads(csvBody, correlationId);

        log.info("profileReadsCsvParsed", correlationId,
            "fileName",    fileName,
            "payloadCount", payloads.size());

        exchange.getIn().setBody(payloads);
    }
}
