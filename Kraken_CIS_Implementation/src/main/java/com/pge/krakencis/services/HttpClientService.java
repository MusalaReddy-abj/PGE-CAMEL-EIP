package com.pge.krakencis.services;

import com.pge.krakencis.exceptions.ErrorCode;
import com.pge.krakencis.exceptions.ExternalServiceException;
import com.pge.krakencis.logging.StructuredLogger;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClient.RequestBodySpec;

import java.net.URI;
import java.util.Objects;

/**
 * Common HTTP sender for both SOAP and REST outbound calls.
 *
 * All domain services (SOARCDCRequestService, future SOAP services, etc.)
 * delegate I/O to this class. No route or processor should use this directly.
 */
@Service
public class HttpClientService {

    private static final StructuredLogger log = StructuredLogger.of(HttpClientService.class);

    private final RestClient restClient;

    public HttpClientService() {
        this.restClient = RestClient.builder().build();
    }

    /**
     * Sends an HTTP request and returns the response.
     * Supports both REST (JSON/XML body) and SOAP (XML envelope + SOAPAction header).
     *
     * @throws ExternalServiceException on non-2xx response or connectivity failure
     */
    public HttpOutboundResponse send(HttpOutboundRequest request, String correlationId) {
        Assert.notNull(request, "HttpOutboundRequest must not be null");
        Assert.hasText(request.getUrl(), "request.url must not be empty");

        long startTime = System.currentTimeMillis();

        log.debug("httpOutboundSending", correlationId,
            "method",      request.getMethod(),
            "url",         request.getUrl(),
            "contentType", request.getContentType());

        try {
            String method      = Objects.requireNonNull(request.getMethod(),      "method");
            String url         = Objects.requireNonNull(request.getUrl(),         "url");
            String contentType   = Objects.requireNonNull(request.getContentType(), "contentType");
            String body          = Objects.requireNonNull(
                                     request.getBody() != null ? request.getBody() : "", "body");
            URI    uri           = Objects.requireNonNull(URI.create(url), "uri");
            String correlationHdr = correlationId != null ? correlationId : "unknown";

            RequestBodySpec spec = restClient
                .method(HttpMethod.valueOf(method))
                .uri(uri)
                .contentType(MediaType.parseMediaType(contentType))
                .header("X-Correlation-ID", correlationHdr);

            // Propagate caller-supplied headers (SOAPAction, Accept, auth, etc.)
            request.getHeaders().forEach((k, v) -> spec.header(k, Objects.requireNonNull(v, k)));

            ResponseEntity<String> response = spec
                .body(Objects.requireNonNull(body, "body"))
                .retrieve()
                .onStatus(status -> !status.is2xxSuccessful(),
                    (req, res) -> {
                        throw new ExternalServiceException(
                            ErrorCode.EXTERNAL_SERVICE_ERROR,
                            url,
                            "HTTP " + res.getStatusCode().value() + " from " + url,
                            correlationId,
                            res.getStatusCode().value(),
                            null);
                    })
                .toEntity(String.class);

            long duration = System.currentTimeMillis() - startTime;
            int  status   = response.getStatusCode().value();

            log.info("httpOutboundCompleted", correlationId,
                "httpStatus", status,
                "durationMs", duration,
                "url",        url);

            return HttpOutboundResponse.builder()
                .statusCode(status)
                .body(response.getBody())
                .success(true)
                .build();

        } catch (ExternalServiceException e) {
            throw e;
        } catch (Exception e) {
            log.error("httpOutboundFailed", correlationId, e,
                "url",     request.getUrl(),
                "message", e.getMessage());
            throw ExternalServiceException.unavailable(request.getUrl(), correlationId, e);
        }
    }
}
