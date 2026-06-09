FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.28.1/opentelemetry-javaagent.jar /app/opentelemetry-javaagent.jar

COPY Kraken_CIS_Implementation/target/kraken-cis-implementation-1.0.0-SNAPSHOT.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", \
  "-javaagent:/app/opentelemetry-javaagent.jar", \
  "-Dotel.service.name=pge-camel-eip", \
  "-Dotel.resource.attributes=deployment.environment=dev,service.version=1.0.0-SNAPSHOT", \
  "-Dotel.exporter.otlp.endpoint=http://otel-collector.observability.svc.cluster.local:4318", \
  "-Dotel.exporter.otlp.protocol=http/protobuf", \
  "-Dotel.exporter.otlp.timeout=5000", \
  "-Dotel.traces.exporter=otlp", \
  "-Dotel.traces.sampler=parentbased_traceidratio", \
  "-Dotel.traces.sampler.arg=0.1", \
  "-Dotel.bsp.max.queue.size=4096", \
  "-Dotel.bsp.max.export.batch.size=1024", \
  "-Dotel.bsp.schedule.delay=2000", \
  "-Dotel.metrics.exporter=none", \
  "-Dotel.logs.exporter=none", \
  "-jar", "app.jar"]
