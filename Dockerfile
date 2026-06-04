FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar /app/opentelemetry-javaagent.jar

COPY Kraken_CIS_Implementation/target/kraken-cis-implementation-1.0.0-SNAPSHOT.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", \
  "-javaagent:/app/opentelemetry-javaagent.jar", \
  "-Dotel.service.name=pge-camel-eip", \
  "-Dotel.exporter.otlp.endpoint=http://otel-collector.observability.svc.cluster.local:4317", \
  "-Dotel.exporter.otlp.protocol=grpc", \
  "-Dotel.traces.exporter=otlp", \
  "-Dotel.metrics.exporter=none", \
  "-Dotel.logs.exporter=none", \
  "-jar", "app.jar"]
