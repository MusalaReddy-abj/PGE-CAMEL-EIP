FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY Kraken_CIS_Implementation/target/kraken-cis-implementation-1.0.0-SNAPSHOT.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
