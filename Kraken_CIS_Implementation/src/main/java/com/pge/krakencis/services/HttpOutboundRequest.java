package com.pge.krakencis.services;

import lombok.Builder;
import lombok.Data;
import lombok.Singular;
import org.springframework.http.MediaType;

import java.util.Map;

/**
 * Protocol-agnostic outbound HTTP request descriptor.
 * Used for both REST (application/json) and SOAP (text/xml) calls.
 *
 * Usage — REST:
 *   HttpOutboundRequest.builder()
 *       .url("http://service/api/resource")
 *       .contentType(MediaType.APPLICATION_JSON_VALUE)
 *       .body(jsonString)
 *       .build();
 *
 * Usage — SOAP:
 *   HttpOutboundRequest.builder()
 *       .url("http://service/ws/endpoint")
 *       .contentType("text/xml; charset=utf-8")
 *       .header("SOAPAction", "\"urn:OperationName\"")
 *       .body(soapEnvelopeXml)
 *       .build();
 */
@Data
@Builder
public class HttpOutboundRequest {

    private String url;

    @Builder.Default
    private String method = "POST";

    @Builder.Default
    private String contentType = MediaType.APPLICATION_JSON_VALUE;

    @Singular
    private Map<String, String> headers;

    private String body;

    @Builder.Default
    private int timeoutMs = 30_000;
}
