package com.pge.krakencis.routes;

import com.pge.krakencis.logging.StructuredLogger;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestBindingMode;
import org.springframework.stereotype.Component;

/**
 * Single, centralised REST DSL configuration for the entire Camel context.
 *
 * Uses Camel property placeholders {{key:default}} instead of @Value to
 * guarantee resolution at Camel startup time (after Spring env is fully loaded).
 */
@Component
public class RestApiConfig extends RouteBuilder {

    private static final StructuredLogger log = StructuredLogger.of(RestApiConfig.class);

    @Override
    public void configure() {
        log.info("restApiConfiguring", null,
            "host", "{{rest.host:0.0.0.0}}",
            "port", "{{rest.port:9080}}");

        restConfiguration()
            .component("undertow")
            .host("{{rest.host:0.0.0.0}}")
            .port("{{rest.port:9080}}")
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
