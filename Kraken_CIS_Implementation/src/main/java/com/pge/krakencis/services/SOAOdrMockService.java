package com.pge.krakencis.services;

import com.pge.krakencis.configs.ExternalServiceProperties;
import com.pge.krakencis.configs.ExternalServiceProperties.ServiceEndpoint;
import com.pge.krakencis.logging.StructuredLogger;
import org.springframework.stereotype.Service;

/**
 * Forwards OnDemandRead requests to the downstream mock SOAP service and
 * returns the raw XML response body to the caller.
 *
 * <p>All connection parameters are resolved at call time from
 * {@link ExternalServiceProperties} under the key {@code odr-mock}.
 */
@Service
public class SOAOdrMockService {

    private static final StructuredLogger log          = StructuredLogger.of(SOAOdrMockService.class);
    private static final String           SERVICE_KEY  = "odr-mock";
    private static final String           SERVICE_NAME = "SOA-ODR-MockService";

    private final HttpClientService         httpClientService;
    private final ExternalServiceProperties externalServiceProperties;

    public SOAOdrMockService(HttpClientService         httpClientService,
                              ExternalServiceProperties externalServiceProperties) {
        this.httpClientService         = httpClientService;
        this.externalServiceProperties = externalServiceProperties;
    }

    /**
     * Posts the OnDemandRead XML request to the mock SOAP service and returns
     * the raw XML response body.
     *
     * @param xmlPayload   well-formed requestMessage XML string
     * @param correlationId tracing ID from the inbound request
     * @return raw XML response body from the mock service
     */
    public String sendRequest(String xmlPayload, String correlationId) {
        ServiceEndpoint endpoint = externalServiceProperties.get(SERVICE_KEY);

        log.info("odrMockServiceSendRequest", correlationId,
            "service", SERVICE_NAME,
            "url",     endpoint.getUrl());

        HttpOutboundRequest request = HttpOutboundRequest.builder()
            .url(endpoint.getUrl())
            .contentType(endpoint.getContentType())
            .header("SOAPAction", "\"\"")
            .body(xmlPayload)
            .timeoutMs(endpoint.getTimeout())
            .serviceName(SERVICE_NAME)
            .maxRetryAttempts(endpoint.getMaxRetryAttempts())
            .build();

        HttpOutboundResponse response = httpClientService.send(request, correlationId);

        int statusCode = response.getStatusCode();

        if (statusCode == 200 && SoapFaultInspector.hasFault(response.getBody())) {
            String  faultMessage  = SoapFaultInspector.extractFaultMessage(response.getBody());
            boolean isClientFault = SoapFaultInspector.isClientFault(response.getBody());
            Integer inferredStatus = SoapFaultInspector.inferHttpStatus(response.getBody());

            log.warn("odrMockSoapFaultDetected", correlationId,
                "service",        SERVICE_NAME,
                "inferredStatus", inferredStatus,
                "isClientFault",  isClientFault,
                "faultMessage",   faultMessage);

            statusCode = inferredStatus != null ? inferredStatus : 500;
        }

        log.info("odrMockServiceResponseReceived", correlationId,
            "service",    SERVICE_NAME,
            "httpStatus", statusCode,
            "success",    statusCode >= 200 && statusCode < 300);

        return response.getBody();
    }
}
