package com.pge.krakencis.routes.rcdc.request;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import org.apache.camel.CamelContext;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for {@link RcdcRequestHttpListner}.
 *
 * <p>Uses WireMock to stub the SOA-RCDC downstream service.
 * Camel routes are fully started — this exercises the real routing, JAXB
 * unmarshal, validation, and Kafka publish path end-to-end (Kafka is
 * stubbed via Spring profiles to use a mock/no-op producer).
 */
@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles("test")
class RcdcRequestHttpListnerTest {

    /** WireMock server on a fixed port matching application-test.yml SOA-RCDC URL. */
    @RegisterExtension
    static WireMockExtension wireMock = WireMockExtension.newInstance()
        .options(wireMockConfig().port(8082))
        .build();

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        // Point SOA-RCDC URL at WireMock
        registry.add("external-services.services.rcdc.url",
            () -> "http://localhost:8082/api/rcdc/command");
        // Use test Kafka config (no-op or embedded)
        registry.add("kafka.producer.brokers", () -> "localhost:9092");
    }

    @Autowired
    private CamelContext camelContext;

    private RestTemplate restTemplate;
    private static final int REST_PORT = 8080;

    @BeforeEach
    void setUp() {
        restTemplate = new RestTemplate();
    }

    // ── Test 1: Valid CONNECT command ─────────────────────────────────────────

    @Test
    void validConnectCommand_returns200XmlAck() {
        // SOA-RCDC stub: accept and return 200
        wireMock.stubFor(post(urlEqualTo("/api/rcdc/command"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"status\":\"OK\"}")));

        String xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <requestMessage xmlns="http://xmlns.oracle.com/ouaf/iec">
              <header>
                <verb>Create</verb>
                <noun>RCDSwitchState</noun>
                <replyAddress>http://hes.pge.com/callback/rcdc</replyAddress>
                <correlationID>test-corr-001</correlationID>
              </header>
              <payLoad>
                <RCDSwitchState>
                  <endDeviceAsset><mRID>1.21.120.0.0.0.0.0.0.0.0.0.0.0.0.0.0.4097</mRID></endDeviceAsset>
                  <state>CONNECT</state>
                </RCDSwitchState>
              </payLoad>
            </requestMessage>
            """;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_XML);
        HttpEntity<String> request = new HttpEntity<>(xml, headers);

        ResponseEntity<String> response = restTemplate.exchange(
            "http://localhost:" + REST_PORT + "/api/v1/rcdc",
            HttpMethod.POST, request, String.class);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).contains("responseDetail");
        assertThat(response.getBody()).contains("replyCode");
        assertThat(response.getHeaders().getFirst("X-Correlation-ID")).isNotBlank();

        // Verify WireMock received exactly one call
        wireMock.verify(1, postRequestedFor(urlEqualTo("/api/rcdc/command")));
    }

    // ── Test 2: Invalid state value → 400 ────────────────────────────────────

    @Test
    void invalidStateValue_returns400WithErrorCode() {
        String xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <requestMessage xmlns="http://xmlns.oracle.com/ouaf/iec">
              <header>
                <verb>Create</verb>
                <noun>RCDSwitchState</noun>
                <replyAddress>http://hes.pge.com/callback/rcdc</replyAddress>
                <correlationID>test-corr-002</correlationID>
              </header>
              <payLoad>
                <RCDSwitchState>
                  <endDeviceAsset><mRID>1.21.120.0.0.0.0.0.0.0.0.0.0.0.0.0.0.4097</mRID></endDeviceAsset>
                  <state>INVALID_STATE</state>
                </RCDSwitchState>
              </payLoad>
            </requestMessage>
            """;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_XML);
        HttpEntity<String> request = new HttpEntity<>(xml, headers);

        assertThatThrownBy(() -> restTemplate.exchange(
                "http://localhost:" + REST_PORT + "/api/v1/rcdc",
                HttpMethod.POST, request, String.class))
            .isInstanceOf(HttpClientErrorException.class)
            .satisfies(e -> {
                HttpClientErrorException ex = (HttpClientErrorException) e;
                assertThat(ex.getStatusCode().value()).isEqualTo(400);
                assertThat(ex.getResponseBodyAsString()).contains("VAL-003");
            });

        // SOA-RCDC should NOT have been called — validation failed before Kafka publish
        wireMock.verify(0, postRequestedFor(urlEqualTo("/api/rcdc/command")));
    }

    // ── Test 3: SOA-RCDC 503 → 503 after retries ─────────────────────────────

    @Test
    void soaRcdcReturns503_returns503AfterRetries() {
        // SOA-RCDC always returns 503 (simulates outage)
        wireMock.stubFor(post(urlEqualTo("/api/rcdc/command"))
            .willReturn(aResponse()
                .withStatus(503)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"error\":\"Service Unavailable\"}")));

        String xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <requestMessage xmlns="http://xmlns.oracle.com/ouaf/iec">
              <header>
                <verb>Create</verb>
                <noun>RCDSwitchState</noun>
                <replyAddress>http://hes.pge.com/callback/rcdc</replyAddress>
                <correlationID>test-corr-003</correlationID>
              </header>
              <payLoad>
                <RCDSwitchState>
                  <endDeviceAsset><mRID>1.21.120.0.0.0.0.0.0.0.0.0.0.0.0.0.0.4097</mRID></endDeviceAsset>
                  <state>DISCONNECT</state>
                </RCDSwitchState>
              </payLoad>
            </requestMessage>
            """;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_XML);
        HttpEntity<String> request = new HttpEntity<>(xml, headers);

        assertThatThrownBy(() -> restTemplate.exchange(
                "http://localhost:" + REST_PORT + "/api/v1/rcdc",
                HttpMethod.POST, request, String.class))
            .isInstanceOf(HttpServerErrorException.class)
            .satisfies(e -> {
                HttpServerErrorException ex = (HttpServerErrorException) e;
                assertThat(ex.getStatusCode().value()).isEqualTo(503);
                String body = ex.getResponseBodyAsString();
                assertThat(body).contains("SYS-001");
                assertThat(body).contains("retriesAttempted");
            });
    }
}
