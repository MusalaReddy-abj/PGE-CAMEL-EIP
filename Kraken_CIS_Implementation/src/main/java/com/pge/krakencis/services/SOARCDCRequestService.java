package com.pge.krakencis.services;

import com.pge.krakencis.configs.ExternalServiceProperties;
import com.pge.krakencis.configs.ExternalServiceProperties.ServiceEndpoint;
import com.pge.krakencis.logging.StructuredLogger;
import org.springframework.stereotype.Service;

/**
 * Service for all outbound calls to the SOA RCDC target service.
 * URL, timeout, and content-type are read from ExternalServiceProperties
 * so they can be overridden per environment without touching this class.
 */
@Service
public class SOARCDCRequestService {

    private static final StructuredLogger log         = StructuredLogger.of(SOARCDCRequestService.class);
    private static final String           SERVICE_KEY = "rcdc";
    private static final String           SERVICE_NAME = "SOA-RCDC-TargetService";

    private final HttpClientService         httpClientService;
    private final ExternalServiceProperties externalServiceProperties;

    public SOARCDCRequestService(HttpClientService         httpClientService,
                                  ExternalServiceProperties externalServiceProperties) {
        this.httpClientService         = httpClientService;
        this.externalServiceProperties = externalServiceProperties;
    }

    /**
     * Sends the pre-mapped RCDC command JSON to the downstream target service.
     * URL and timeout are resolved dynamically from properties at call time.
     *
     * @param jsonPayload   serialised RcdcTargetRequest JSON
     * @param correlationId tracing ID from the inbound request
     * @return HTTP status code returned by the target
     */
    public int sendCommand(String jsonPayload, String correlationId) {
        ServiceEndpoint endpoint = externalServiceProperties.get(SERVICE_KEY);

        log.info("rcdcServiceSendCommand", correlationId,
            "service", SERVICE_NAME,
            "url",     endpoint.getUrl());

        HttpOutboundRequest request = HttpOutboundRequest.builder()
            .url(endpoint.getUrl())
            .contentType(endpoint.getContentType())
            .body(jsonPayload)
            .timeoutMs(endpoint.getTimeout())
            .build();

        HttpOutboundResponse response = httpClientService.send(request, correlationId);

        log.info("rcdcServiceCommandSent", correlationId,
            "service",    SERVICE_NAME,
            "httpStatus", response.getStatusCode(),
            "success",    response.isSuccess());

        return response.getStatusCode();
    }
}
