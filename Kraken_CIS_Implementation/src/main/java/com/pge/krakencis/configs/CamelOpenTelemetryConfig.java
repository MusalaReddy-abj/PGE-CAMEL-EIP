package com.pge.krakencis.configs;

import com.pge.krakencis.logging.StructuredLogger;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Configuration;

/**
 * Bridges the Spring-managed OpenTelemetry SDK to Camel's OpenTelemetry instrumentation.
 *
 * <p><b>Why this is needed.</b> Spring Boot's {@code micrometer-tracing-bridge-otel}
 * auto-configures a fully-wired {@link OpenTelemetry} SDK (sampler + OTLP exporter +
 * resource) but it does <em>not</em> register it as the JVM-global instance. Apache
 * Camel's OpenTelemetry tracer resolves its tracer from {@link GlobalOpenTelemetry},
 * which therefore returns a <b>no-op</b> — so Camel routes/processors produce <b>no
 * spans</b> and you only see the single inbound HTTP span.
 *
 * <p>Registering the Spring SDK as global here makes camel-opentelemetry instrument
 * every route and processor with the same exporter and resource, giving the full
 * multi-span trace (HTTP → route → processors → publish).
 */
@Configuration
public class CamelOpenTelemetryConfig {

    private static final StructuredLogger log = StructuredLogger.of(CamelOpenTelemetryConfig.class);

    private final ObjectProvider<OpenTelemetry> openTelemetryProvider;

    public CamelOpenTelemetryConfig(ObjectProvider<OpenTelemetry> openTelemetryProvider) {
        this.openTelemetryProvider = openTelemetryProvider;
    }

    @PostConstruct
    public void registerGlobal() {
        OpenTelemetry openTelemetry = openTelemetryProvider.getIfAvailable();
        if (openTelemetry == null) {
            log.warn("camelOtelNoOpenTelemetryBean", null,
                "detail", "no OpenTelemetry bean — Camel route spans will not be produced");
            return;
        }
        try {
            GlobalOpenTelemetry.set(openTelemetry);
            log.info("camelOtelGlobalRegistered", null,
                "detail", "Spring OpenTelemetry SDK registered as GlobalOpenTelemetry for Camel");
        } catch (IllegalStateException alreadySet) {
            // GlobalOpenTelemetry can only be set once; if something already set it, that's fine.
            log.info("camelOtelGlobalAlreadySet", null, "detail", alreadySet.getMessage());
        }
    }
}
