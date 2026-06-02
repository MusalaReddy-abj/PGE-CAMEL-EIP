package com.pge.krakencis.routes;

import com.pge.krakencis.logging.StructuredLogger;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestBindingMode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Single, centralised REST DSL configuration for the entire Camel context.
 *
 * Uses Spring @Value to inject host/port so the values are fully resolved
 * by Spring Boot's environment BEFORE Camel's context starts — avoiding the
 * race condition where Camel's own property bridge is not yet active during
 * restConfiguration() setup, which caused the port to default to 8080.
 */
@Component
public class RestApiConfig extends RouteBuilder {

    private static final StructuredLogger log = StructuredLogger.of(RestApiConfig.class);

    @Value("${rest.host:0.0.0.0}")
    private String restHost;

    @Value("${rest.port:9080}")
    private int restPort;

    @Override
    public void configure() {
        log.info("restApiConfiguring", null, "host", restHost, "port", restPort);

        restConfiguration()
            .component("undertow")
            .host(restHost)
            .port(restPort)
            .bindingMode(RestBindingMode.json)
            .dataFormatProperty("prettyPrint", "true")
            .dataFormatProperty("allowUnmarshallType", "true")
            .enableCORS(true)
            .corsHeaderProperty("Access-Control-Allow-Origin", "*")
            .corsHeaderProperty("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
            .corsHeaderProperty("Access-Control-Allow-Headers",
                                "Content-Type,Authorization,X-Correlation-ID")
            .contextPath("/api/v1")
            .apiContextPath("/api-doc")
            .apiProperty("api.title", "Kraken CIS Integration API")
            .apiProperty("api.version", "1.0.0");
    }
}
