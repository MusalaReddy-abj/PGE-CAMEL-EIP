# Contributing to Kraken CIS Integration

Welcome. This guide gets you from zero to a running local environment and covers the
conventions for adding new integrations.

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Java JDK | 17 | `brew install temurin17` / [adoptium.net](https://adoptium.net) |
| Maven | 3.9+ | `brew install maven` |
| Docker + Compose | Latest | [docker.com](https://www.docker.com/products/docker-desktop) |
| Kafka CLI | 3.x | Included in Kafka distribution |
| curl + jq | Any | `brew install jq` |

---

## 1. Local Setup

```bash
# Clone
git clone https://github.com/pge/kraken-cis-implementation.git
cd kraken-cis-implementation

# Build (skip tests on first run)
mvn compile -q

# Start Kafka + ActiveMQ via Docker
docker compose -f deployment/docker/docker-compose.yml up -d
```

### Configure credentials

Copy and edit the template configs:
```bash
cp src/main/resources/s3.yml.example src/main/resources/s3.yml
# Edit s3.yml with your AWS credentials (this file is gitignored)
```

For FTP, edit `src/main/resources/ftp.yml` to point at your FTP server.

### Run the application

```bash
# Dev profile (console + file logging, 100% trace sampling)
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=dev"

# Or directly
java -jar target/kraken-cis-implementation-*.jar --spring.profiles.active=dev
```

**Ports after startup:**
- `8080` — REST API (`/api/v1/...`)
- `9091` — Actuator (`/actuator/health`, `/actuator/camelroutes`)

Verify:
```bash
curl http://localhost:9091/actuator/health | jq .status
# → "UP"
```

---

## 2. Running Without Docker

If you have Kafka running locally already:

```yaml
# src/main/resources/kafka.yml — update broker
kafka:
  producer:
    brokers: localhost:9092
```

If you don't need FTP polling:
- The FTP route only starts if `ftp.profile-reads.connection.host` resolves
- Set `ftp.profile-reads.delay-ms=999999999` to effectively disable polling

If you don't need S3:
- S3 listener only starts when `aws.s3.profile-reads.bucket-name` is configured
- Leave `s3.yml` absent (it's in `.gitignore` — the `optional:` import means the app still starts)

---

## 3. Running Tests

```bash
# All tests + JaCoCo coverage report
mvn test

# Single test class
mvn test -Dtest=RcdcRequestHttpListnerTest

# View coverage report
open target/site/jacoco/index.html
```

### WireMock pattern

Tests use [WireMock](https://wiremock.org) to stub downstream services. Example stub:

```java
@RegisterExtension
static WireMockExtension wireMock = WireMockExtension.newInstance()
    .options(wireMockConfig().port(8082))
    .build();

wireMock.stubFor(post(urlEqualTo("/api/rcdc/command"))
    .willReturn(aResponse().withStatus(200).withBody("{\"status\":\"OK\"}")));
```

The test profile (`application-test.yml`) points all external URLs at `localhost:8082/8083`.

---

## 4. Adding a New Integration Flow

Follow these steps in order. Use the RCDC flow as a reference.

### Step 1 — Models

```
src/main/java/com/pge/krakencis/models/{domain}/
  ├── MyRequest.java        @Data @NoArgsConstructor @JsonIgnoreProperties(ignoreUnknown=true)
  └── MyResponse.java       @Data @Builder
```

- Use `@JsonIgnoreProperties(ignoreUnknown = true)` on inbound models (tolerant reader)
- Use `@Builder` + `@JsonInclude(NON_NULL)` on outbound models

### Step 2 — Exception classification

Decide up front which exceptions map to which HTTP status / DLQ routing:

```java
// Payload invalid → 400 / DLQ immediately
throw ValidationException.missingField("fieldName", correlationId);

// Mapping error → 422 / DLQ immediately
throw TransformationException.mappingFailed("sourceField", "targetField", correlationId, cause);

// Transient service error → retry 3× → retry queue
throw RetryableException.transient_("Service returned HTTP " + status, correlationId);

// Permanent 4xx error → DLQ immediately
throw ExternalServiceException.httpError("MyService", status, responseBody, correlationId);
```

### Step 3 — Processor

```
src/main/java/com/pge/krakencis/processors/{domain}/
  └── MyRequestProcessor.java   extends BaseProcessor
```

```java
@Component
public class MyRequestProcessor extends BaseProcessor {

    @Override
    public void process(Exchange exchange) throws Exception {
        String correlationId = exchange.getProperty(PROP_CORRELATION_ID, String.class);
        MyRequest request = exchange.getIn().getBody(MyRequest.class);

        validate(request, correlationId);    // throw ValidationException if invalid
        MyResult result = map(request);      // throw TransformationException if mapping fails

        exchange.getIn().setBody(result);
    }
}
```

### Step 4 — HTTP Route (for inbound REST)

```java
@Component
public class MyHttpListner extends BaseRoute {

    @Value("${kafka.topic.my-topic}")
    private String myTopic;

    @Override
    public void configure() {
        rest().post("/my-endpoint")
            .type(MyRequest.class)
            .to("direct:process-my-endpoint");

        processingRoute("direct:process-my-endpoint", "route-my-endpoint", "myOperation", route ->
            route
                .process(myProcessor)
                .setProperty(KAFKA_TOPIC, constant(myTopic))
                .to("direct:publishToKafka")
                .process(myResponseProcessor)
        );
    }
}
```

### Step 5 — Kafka Consumer Route (for async downstream call)

```java
@Component
public class MyKafkaConsumer extends BaseKafkaConsumerRoute {

    @Value("${kafka.topic.my-retry:kraken-my-retry-events}")
    private String retryTopic;

    @Value("${kafka.topic.my-dlq:kraken-my-dlq-events}")
    private String dlqTopic;

    @Override
    public void configure() {
        RouteDefinition route = from("kafka:{{kafka.topic.my-topic}}"
            + "?brokers={{kafka.producer.brokers}}&autoCommitEnable=false&allowManualCommit=true")
            .routeId("route-my-kafka-consumer");

        configureKafkaErrorHandlers(route, retryTopic, dlqTopic, "MY-SERVICE");

        route
            .process(e -> e.setProperty(PROP_ORIGINAL_BODY, e.getIn().getBody(String.class)))
            .process(e -> extractCorrelationId(e, "myMessageConsumed"))
            .process(routeLoggingProcessor.entry(OPERATION))
            .process(myCallProcessor)
            .process(this::commitOffset)
            .process(routeLoggingProcessor.exit(OPERATION));
    }
}
```

### Step 6 — Kafka topics

```yaml
# kafka.yml
kafka:
  topic:
    my-topic:  kraken-my-events
    my-retry:  kraken-my-retry-events
    my-dlq:    kraken-my-dlq-events
```

### Step 7 — Tests

```
src/test/java/com/pge/krakencis/routes/{domain}/
  └── MyHttpListnerTest.java
```

Minimum test coverage:
- ✅ Happy path (valid request → correct response + Kafka message published)
- ✅ Validation failure (missing required field → HTTP 400 + errorCode)
- ✅ Downstream 503 → HTTP 503 with `retriesAttempted`

### Step 8 — OpenAPI

Add the new endpoint to `docs/openapi.yaml`:
- Request schema under `components/schemas`
- Response schemas (success + error)
- Path entry under `paths`
- Error responses referencing the shared `ErrorResponse` component

### Step 9 — Kong

Add service + route + plugins to `docs/kong.yml`:
- Rate limiting
- Request size limiting
- Correlation ID injection

---

## 5. Code Conventions

### Exceptions — always use typed exceptions

```java
// ✅ Correct
throw ValidationException.missingField("deviceId", correlationId);

// ❌ Wrong — generic exception loses error code and context
throw new IllegalArgumentException("deviceId is required");
```

### Logging — always use StructuredLogger

```java
private static final StructuredLogger log = StructuredLogger.of(MyClass.class);

// ✅ Correct — structured key-value pairs
log.info("myEvent", correlationId, "key1", value1, "key2", value2);

// ❌ Wrong — plain string interpolation loses structure
logger.info("Processing " + correlationId + " with key=" + value1);
```

### No inline magic strings — use LogConstants

```java
// ✅ Correct
exchange.setProperty(LogConstants.PROP_CORRELATION_ID, cid);
exchange.setProperty(LogConstants.KAFKA_TOPIC, topic);

// ❌ Wrong
exchange.setProperty("X-Correlation-ID", cid);
exchange.setProperty("kafkaTopic", topic);
```

### No comments that repeat the code

```java
// ❌ Bad — the comment just restates what the code says
// Validate the request
validate(request, correlationId);

// ✅ Good — comment explains WHY, not WHAT
// RcdcState.from() normalises case — "connect" and "CONNECT" both accepted
RcdcState state = RcdcState.from(rawState);
```

---

## 6. Environment Promotion Checklist

Before promoting from one environment to the next:

- [ ] All tests pass (`mvn test`)
- [ ] JaCoCo coverage report reviewed (no significant regression)
- [ ] `application-{env}.yml` updated with environment-specific URLs and credentials
- [ ] `kafka.yml` topic configuration verified for target environment
- [ ] `docs/kong.yml` rate limits reviewed for target environment traffic
- [ ] S3 credentials rotated (if promoting to UAT/prod)
- [ ] Docker image tagged with git SHA
- [ ] Kubernetes `deployment.yaml` updated with new image tag
- [ ] Smoke test: `curl /actuator/health` returns UP after deploy
- [ ] DLQ depths are zero before promoting (no outstanding failures)
