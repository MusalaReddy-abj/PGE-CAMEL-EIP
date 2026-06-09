FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# OpenTelemetry Java Agent Migration
# Native SDK removed / Instrumentation provided by Java Agent.
# Download a pinned OpenTelemetry Java Agent so the image is self-contained.
# All auto-instrumentation (Spring Boot, Camel, Kafka producer/consumer, HTTP
# clients, REST/Undertow) and OTLP export are provided by this agent.
ARG OTEL_AGENT_VERSION=2.10.0
ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v${OTEL_AGENT_VERSION}/opentelemetry-javaagent.jar /app/opentelemetry-javaagent.jar

COPY target/kraken-cis-implementation-*.jar app.jar

RUN addgroup -S appgroup && adduser -S appuser -G appgroup \
    && chmod 644 /app/opentelemetry-javaagent.jar
USER appuser

EXPOSE 8080

# OpenTelemetry Java Agent Migration
# service.name = PGE_IMP_DEV_MAIN, OTLP/gRPC → OTel Collector :4317 → Jaeger.
# Metrics/logs exporters disabled (traces only), matching the deployment config.
ENTRYPOINT [ \
    "java", \
    "-javaagent:/app/opentelemetry-javaagent.jar", \
    "-Dotel.service.name=PGE_IMP_DEV_MAIN", \
    "-Dotel.exporter.otlp.endpoint=http://otel-collector.observability.svc.cluster.local:4317", \
    "-Dotel.exporter.otlp.protocol=grpc", \
    "-Dotel.traces.exporter=otlp", \
    "-Dotel.metrics.exporter=none", \
    "-Dotel.logs.exporter=none", \
    "-jar", \
    "app.jar" \
]
