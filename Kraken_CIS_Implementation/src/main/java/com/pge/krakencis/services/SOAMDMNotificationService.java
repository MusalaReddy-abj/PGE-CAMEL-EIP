package com.pge.krakencis.services;

import com.pge.krakencis.configs.ExternalServiceProperties;
import com.pge.krakencis.configs.ExternalServiceProperties.ServiceEndpoint;
import com.pge.krakencis.logging.StructuredLogger;
import org.springframework.stereotype.Service;

/**
 * Service for all outbound calls to the MDM (Meter Data Management) SOAP service.
 * Sends RCDC execution notifications as SOAP XML RequestMessage payloads.
 * Delegates HTTP I/O to the common HttpClientService.
 */
@Service
public class SOAMDMNotificationService {

    private static final StructuredLogger log          = StructuredLogger.of(SOAMDMNotificationService.class);
    private static final String           SERVICE_KEY  = "rcdc-mdm-notification";
    private static final String           SERVICE_NAME = "SOA-MDM-NotificationService";

    private final HttpClientService         httpClientService;
    private final ExternalServiceProperties externalServiceProperties;

    public SOAMDMNotificationService(HttpClientService         httpClientService,
                                      ExternalServiceProperties externalServiceProperties) {
        this.httpClientService         = httpClientService;
        this.externalServiceProperties = externalServiceProperties;
    }

    /**
     * Posts the SOAP XML RequestMessage to the MDM endpoint.
     *
     * @param soapXmlPayload well-formed XML string (mapped from HES response)
     * @param correlationId  tracing ID propagated end-to-end
     * @return HTTP status code returned by MDM
     */
    public int sendNotification(String soapXmlPayload, String correlationId) {
        ServiceEndpoint endpoint = externalServiceProperties.get(SERVICE_KEY);

        log.info("mdmNotificationStarted", correlationId,
            "service", SERVICE_NAME,
            "url",     endpoint.getUrl());

        HttpOutboundRequest request = HttpOutboundRequest.builder()
            .url(endpoint.getUrl())
            .contentType(endpoint.getContentType())    // text/xml; charset=utf-8
            .header("SOAPAction", "\"\"")              // empty SOAPAction for doc/literal
            .body(soapXmlPayload)
            .timeoutMs(endpoint.getTimeout())
            .serviceName(SERVICE_NAME)
            .maxRetryAttempts(endpoint.getMaxRetryAttempts())
            .build();

        HttpOutboundResponse response = httpClientService.send(request, correlationId);

        int statusCode = response.getStatusCode();

        // SOAP services can return HTTP 200 with a <soap:Fault> inside the body.
        // Inspect the response body and override the status so retry/DLQ routing
        // works the same as for plain HTTP errors.
        if (statusCode == 200 && SoapFaultInspector.hasFault(response.getBody())) {
            Integer inferredStatus = SoapFaultInspector.inferHttpStatus(response.getBody());
            String  faultMessage   = SoapFaultInspector.extractFaultMessage(response.getBody());
            boolean isClientFault  = SoapFaultInspector.isClientFault(response.getBody());

            log.warn("soapFaultDetectedInHttpOkResponse", correlationId,
                "service",       SERVICE_NAME,
                "inferredStatus", inferredStatus,
                "isClientFault",  isClientFault,
                "faultMessage",   faultMessage);

            statusCode = inferredStatus != null ? inferredStatus : 500;
        }

        log.info("mdmNotificationSent", correlationId,
            "service",    SERVICE_NAME,
            "httpStatus", statusCode,
            "success",    statusCode >= 200 && statusCode < 300);

        return statusCode;
    }
}
