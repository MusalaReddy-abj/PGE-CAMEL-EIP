FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY target/kraken-cis-implementation-*.jar app.jar

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
