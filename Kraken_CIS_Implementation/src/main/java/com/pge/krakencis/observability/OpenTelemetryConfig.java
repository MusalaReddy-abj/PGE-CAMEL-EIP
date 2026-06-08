package com.pge.krakencis.observability;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * OpenTelemetry Integration — centralised Native OpenTelemetry SDK configuration.
 *
 * <p>Builds an independent {@link OpenTelemetrySdk} that exports the custom processing-stage
 * spans (RECEIVED / VALIDATED / KAFKA_PUBLISH_* / EXTERNAL_SERVICE_CALL / RESPONSE_SENT)
 * created by {@link TracingHelper}. It is fully <b>additive</b>:
 *
 * <ul>
 *   <li>It does not modify, replace, or disable the existing Micrometer-bridge /
 *       camel-observation auto-instrumentation. Both run side by side.</li>
 *   <li>Custom spans are created as children of {@code Context.current()}, so they nest
 *       under the auto-instrumented Camel spans into a single connected trace.</li>
 *   <li>Registering {@link GlobalOpenTelemetry} is best-effort and guarded — if Spring's
 *       bridge has already registered a global instance, this SDK is used locally instead
 *       (helpers read the {@link OpenTelemetrySdk} bean, never the global), so startup
 *       never fails.</li>
 * </ul>
 *
 * <p>All configuration is read from {@code application.yml} ({@code otel.tracing.*}) with
 * environment-variable overrides. No business logic changes.
 */
@Configuration
public class OpenTelemetryConfig {

    private static final Logger log = LoggerFactory.getLogger(OpenTelemetryConfig.class);

    @Value("${otel.tracing.enabled:true}")
    private boolean enabled;

    /** Collector base endpoint, e.g. http://localhost:4318. "/v1/traces" is appended if absent. */
    @Value("${otel.tracing.endpoint:http://localhost:4318}")
    private String endpoint;

    @Value("${otel.tracing.service-name:camel-rcd-service-dev-main}")
    private String serviceName;

    @Value("${otel.tracing.service-namespace:pge}")
    private String serviceNamespace;

    @Value("${otel.tracing.environment:dev}")
    private String environment;

    private OpenTelemetrySdk openTelemetrySdk;

    /**
     * Exposes the Native OpenTelemetry SDK as a Spring bean and wires the static helpers.
     * Returns a no-op-friendly SDK reference; when tracing is disabled the helpers simply
     * skip span creation.
     */
    @Bean
    public OpenTelemetrySdk nativeOpenTelemetrySdk() {
        return openTelemetrySdk;
    }

    @PostConstruct
    void init() {
        if (!enabled) {
            log.info("Native OpenTelemetry SDK tracing is DISABLED (otel.tracing.enabled=false)");
            return;
        }

        String tracesEndpoint = endpoint.endsWith("/v1/traces") ? endpoint : endpoint + "/v1/traces";

        // Resource attributes — identify these spans in Jaeger/Tempo.
        Resource resource = Resource.getDefault().merge(Resource.create(Attributes.of(
                AttributeKey.stringKey("service.name"),      serviceName,
                AttributeKey.stringKey("service.namespace"), serviceNamespace,
                AttributeKey.stringKey("deployment.environment"), environment)));

        // OTLP/HTTP exporter → OpenTelemetry Collector.
        OtlpHttpSpanExporter exporter = OtlpHttpSpanExporter.builder()
                .setEndpoint(tracesEndpoint)
                .setTimeout(Duration.ofSeconds(10))
                .build();

        // BatchSpanProcessor — buffers and ships spans asynchronously; export failures are
        // non-fatal (spans are dropped, the request path is never blocked).
        BatchSpanProcessor spanProcessor = BatchSpanProcessor.builder(exporter)
                .setScheduleDelay(Duration.ofSeconds(2))
                .build();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(spanProcessor)
                .setResource(resource)
                .build();

        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();

        // Register GlobalOpenTelemetry — best-effort. If Spring's Micrometer bridge already
        // set a global instance, this throws; we catch it and keep using the local SDK bean.
        try {
            GlobalOpenTelemetry.set(sdk);
            log.info("Native OpenTelemetry SDK registered as GlobalOpenTelemetry");
        } catch (IllegalStateException alreadySet) {
            log.info("GlobalOpenTelemetry already registered (Micrometer bridge); "
                    + "Native SDK will be used via the local bean only");
        }

        this.openTelemetrySdk = sdk;
        TracingHelper.init(sdk);
        SpanAttributeHelper.setServiceName(serviceName);

        log.info("Native OpenTelemetry SDK initialised — service.name={}, endpoint={}",
                serviceName, tracesEndpoint);
    }

    @PreDestroy
    void shutdown() {
        if (openTelemetrySdk != null) {
            // Flush buffered spans before JVM exit.
            openTelemetrySdk.getSdkTracerProvider().shutdown();
            log.info("Native OpenTelemetry SDK shut down (spans flushed)");
        }
    }
}
