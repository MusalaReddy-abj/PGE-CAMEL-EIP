# Kraken CIS Integration Platform — Demo Guide

**Technology:** Apache Camel 4.6 · Spring Boot 3.2 · Kafka · AWS S3 · FTP · Kong Gateway  
**Version:** 1.0.0 · **Date:** 2025

---

## Table of Contents
1. [Architecture Overview](#1-architecture-overview)
2. [Integration Patterns Implemented](#2-integration-patterns-implemented)
3. [Flow Walkthroughs](#3-flow-walkthroughs)
4. [Error Handling Strategy](#4-error-handling-strategy)
5. [Retry & Dead-Letter Architecture](#5-retry--dead-letter-architecture)
6. [Large Payload & File Handling](#6-large-payload--file-handling)
7. [Scalability Design](#7-scalability-design)
8. [Gateway & Policy Enforcement](#8-gateway--policy-enforcement)
9. [Observability & Tracing](#9-observability--tracing)
10. [Best Practices Reference](#10-best-practices-reference)

---

## 1. Architecture Overview

```
                     ┌────────────────────────────────────────────────────────┐
                     │               Kong API Gateway (:8000)                 │
                     │  Rate-limit · Size-limit · CORS · Correlation-ID       │
                     └──────────┬───────────────────┬──────────────────┬──────┘
                                │                   │                  │
                         POST /alarms         POST /rcdc      POST /rcdc/response
                                │                   │                  │
                     ┌──────────▼───────────────────▼──────────────────▼──────┐
                     │        Kraken CIS Integration App (:8080)              │
                     │                  Apache Camel 4.6                      │
                     │                                                         │
                     │  AlarmHttpListner  RcdcRequestHttpListner  RcdcHesResp │
                     │         │                  │                    │       │
                     │         └──────────────────┼────────────────────┘       │
                     │                            │                            │
                     │              direct:publishToKafka                      │
                     └────────────────────────────┼────────────────────────────┘
                                                  │
            ┌─────────────────────────────────────┼────────────────────────────┐
            │                   KAFKA (192.168.4.34:9092)                      │
            │                                     │                            │
            │  kraken-alarm-events   kraken-rcdc-events   kraken-rcdc-hes-*   │
            │  kraken-*-retry-events              │   kraken-*-dlq-events      │
            └─────────────────┬───────────────────┼──────────────────┬─────────┘
                              │                   │                  │
                    ┌─────────▼──────┐  ┌─────────▼──────┐  ┌───────▼──────┐
                    │  (downstream   │  │  SOA-RCDC       │  │  MDM SOAP    │
                    │   consumers)   │  │  HTTP Service   │  │  Service     │
                    └────────────────┘  └─────────────────┘  └──────────────┘

            FTP Server ─────────────────────────────────────────────────────┐
            S3 Bucket  ─────┐  ProfileReadsFTPListner / S3Listner           │
            (pge-int-demo)  └──► ProfileReadsCsvProcessor ──► Kafka         │
                                                                             │
```

**Two transport patterns run side by side:**
- **Synchronous REST** — caller gets an immediate acknowledgement; Kafka decouples downstream processing
- **Asynchronous Kafka** — downstream services are called independently with full retry/DLQ protection
- **Batch/File** — FTP and S3 polling with per-row error isolation and configurable batch publishing

---

## 2. Integration Patterns Implemented

### Enterprise Integration Patterns (EIP)

| Pattern | Where | Code Reference |
|---------|-------|----------------|
| **Message Channel** | Kafka topics as durable, partitioned channels | `kafka.yml` — 9 topics |
| **Dead Letter Channel** | Per-flow DLQ topics with original body preserved | `KafkaErrorRoutes.java` |
| **Content-Based Router** | Route to DLQ vs retry queue based on exception type | `BaseKafkaConsumerRoute.configureKafkaErrorHandlers()` |
| **Message Translator** | RCDC XML → JSON, CSV → Kafka payload, HES → SOAP | `RcdcMapper`, `ProfileReadsMapper`, `RcdcMdmNotificationMapper` |
| **Correlation Identifier** | UUID propagated across HTTP headers, Kafka keys, SOAP | `CorrelationIdProcessor` |
| **Splitter** | CSV rows split into individual Kafka messages | `ProfileReadsCsvProcessor` + `KafkaRoute.split()` |
| **Gateway** | Kong enforces rate-limit, CORS, size-limit centrally | `docs/kong.yml` |
| **Retry** | 3× exponential backoff + retry-queue consumer | `BaseKafkaConsumerRoute`, `RcdcRetryKafkaRoute` |

---

## 3. Flow Walkthroughs

### 3.1 Alarm Events Flow

**Entry point:** `POST /api/v1/alarms` (application/json)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  AlarmHttpListner.configure()                                               │
│                                                                             │
│  rest().post("/alarms")                                                     │
│        .type(AlarmRequest.class)          ← Camel auto-deserialises JSON   │
│        .to("direct:process-alarms");      ← hands off to direct route      │
│                                                                             │
│  processingRoute("direct:process-alarms", ..., route ->                     │
│      route                                                                  │
│        .process(alarmEventProcessor)      ← Step 1: validate + map         │
│        .setProperty(KAFKA_TOPIC, ...)     ← Step 2: set target topic       │
│        .to("direct:publishToKafka")       ← Step 3: publish to Kafka       │
│        .process(alarmResponseProcessor)   ← Step 4: HTTP 202 response      │
│  );                                                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

**AlarmEventProcessor** — validates 3 mandatory fields per event:
```java
// ValidationException thrown → HTTP 400, no Kafka write
if (event.getDeviceIdentifierNumber() == null) throw ValidationException.missingField(...);
if (event.getCategory() == null)               throw ValidationException.missingField(...);
if (event.getCreatedDateTime() == null)         throw ValidationException.missingField(...);
```

**AlarmEventMapper** — maps each `AlarmEvent` → `KrakenEvent`:
```
deviceIdentifierNumber → externalId + messageId ("Alarms_MTR-001")
category               → extEventName
createdDateTime        → eventDate (yyyy-MM-dd, parsed from ISO-8601+offset)
hesCode                → "TRILLIANT"  (hardcoded constant)
```

**HTTP 202 Response:**
```json
{ "status": "ACCEPTED", "correlationId": "...", "eventsPublished": 3, "topic": "kraken-alarm-events" }
```

---

### 3.2 RCDC Remote Connect/Disconnect Flow

**Entry point:** `POST /api/v1/rcdc` (application/xml)

**Stage A — Synchronous (caller waits for acknowledgement):**
```
┌─────────────────────────────────────────────────────────────────────────────┐
│  1. JAXB Unmarshal   XML String → RcdcRequest Java object                  │
│     Namespace: http://xmlns.oracle.com/ouaf/iec                             │
│                                                                             │
│  2. RcdcRequestProcessor                                                    │
│     - Validates: header, correlationID, mRID, state                        │
│     - RcdcState.from(state) — accepts "connect"/"CONNECT" (case-insensitive)│
│     - Stores: correlationId, mRID, state as exchange properties             │
│                                                                             │
│  3. RcdcTargetMappingProcessor                                              │
│     - RcdcRequest → RcdcTargetRequest                                       │
│     - ADDS: header.timestamp = OffsetDateTime.now() (ISO-8601)             │
│     - ADDS: header.source    = "KRAKEN-CIS"                                │
│                                                                             │
│  4. KafkaRoute — publish to kraken-rcdc-events                             │
│     Key = correlationId (ensures ordered delivery per meter)               │
│                                                                             │
│  5. RcdcAcknowledgementProcessor                                            │
│     - Generates random 10-digit instanceID                                  │
│     - Returns XML 200: <responseDetail><instanceID>...</instanceID>         │
│                         <reply><replyCode>0.0</replyCode></reply>           │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Stage B — Asynchronous (Kafka consumer → SOA-RCDC):**
```
┌─────────────────────────────────────────────────────────────────────────────┐
│  RcdcRequestKafkaListner (consumer group: kraken-cis-group)                 │
│                                                                             │
│  1. Save original body → PROP_ORIGINAL_BODY  (needed for DLQ replay)       │
│  2. Extract correlationId from Kafka message key                            │
│  3. RcdcTargetCallProcessor → HTTP POST to SOA-RCDC                        │
│  4. RcdcTargetResponseProcessor → check HTTP status                        │
│     2xx       → commit offset ✓                                             │
│     5xx/404   → RetryableException → retry 3× → retry queue                │
│     4xx other → ExternalServiceException → DLQ immediately                 │
│     network   → RetryableException → retry 3× → retry queue                │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### 3.3 HES Response Callback Flow

**Entry point:** `POST /api/v1/rcdc/response` (application/json)

```
HES sends result → Kong → RcdcHesResponseHttpListner
    │
    ├─ Validates RcdcHesResponseMessage (Header, Reply, Payload all required)
    ├─ Extracts correlationID from Header.CorrelationID
    │  (falls back to X-Correlation-ID request header if absent)
    ├─ Publishes to kraken-rcdc-hes-response-events
    └─ Returns HTTP 202 Accepted

Asynchronously:
    RcdcResponseHesKafkaListner → RcdcMdmNotificationProcessor
        ├─ Deserialise JSON → RcdcHesResponseMessage
        ├─ Map → SOAP XML envelope via RcdcMdmNotificationMapper
        ├─ SoapFaultInspector checks HTTP 200 responses for soap:Fault
        │   soap:Server → treated as 500 → retry
        │   soap:Client → treated as 400 → DLQ
        └─ SOAMDMNotificationService.sendNotification(soapXml, correlationId)
```

---

### 3.4 Profile Reads — Batch File Ingestion

**Dual transport: FTP + AWS S3 (both active simultaneously)**

```
FTP: 192.168.4.34:21 / Reads/profilereads/*.csv  (polls every 60s)
S3:  s3://pge-int-demo/Reads/profilereads/        (polls every 60s)
         │
         ▼
ProfileReadsCsvProcessor
    ├─ File-size guard: rejects files > 100 MB (reads CamelFileSize / CamelAwsS3ContentLength)
    ├─ Streams file line-by-line (BufferedReader — never full file in heap)
    ├─ Validates CSV header (6 required columns: Verb,Noun,mRID,ReadingType,timeStamp,value)
    ├─ Per-row parsing in try/catch — failed rows collected in PROP_FAILED_ROWS
    ├─ Every 500 rows → publish batch to Kafka via ProducerTemplate (memory bounded)
    └─ Final batch → exchange body → route publishes

    ProfileReadsMapper.toKafkaMessages()
        Groups by mRID + ReadingType → one Kafka message per combination:
        {
          "externalId": "AW8003090",         ← from mRID
          "sourceHes":  "SENSUS",            ← constant
          "profileName":"INTERVAL_READS",    ← constant
          "registers": [{
            "readingType": "0.0.0.4...",     ← from ReadingType
            "readings": [
              { "qualityType":"1.0.0", "value":0.0, "timestamp":"2025-12-18T00:30:00+05:30" },
              { "qualityType":"1.0.0", "value":0.0, "timestamp":"2025-12-18T01:00:00+05:30" }
            ]
          }]
        }

    Failed rows → direct:publishProfileReadsDlq → kraken-profile-reads-dlq-events
    Success     → FTP: .done/  |  S3: Reads/Archive/
    Error       → FTP: .error/ |  S3: Reads/Error/
```

---

## 4. Error Handling Strategy

### Single Exception Hierarchy

```
KrakenBaseException (abstract RuntimeException)
    ├─ ValidationException      → HTTP 400  (VAL-xxx codes)
    ├─ TransformationException  → HTTP 422  (TRF-xxx codes)
    ├─ ExternalServiceException → HTTP 500  (EXT-xxx codes)
    ├─ IntegrationException     → HTTP 500  (INT-xxx codes)
    ├─ SystemException          → HTTP 500  (SYS-xxx codes)
    └─ RetryableException       → retry     (RET-xxx codes)
```

**Best Practice — Typed exceptions map directly to HTTP status codes:**
```java
// BaseRoute.processingRoute() — all HTTP listeners auto-wired
route.onException(ValidationException.class)
    .handled(true).process(exceptionProcessor.validation(operation)).end();

route.onException(TransformationException.class)
    .handled(true).process(exceptionProcessor.transformation(operation)).end();

route.onException(Exception.class)
    .maximumRedeliveries(3).redeliveryDelay(1_000).backOffMultiplier(2)
    .handled(true).process(exceptionProcessor.systemWithRetryInfo(operation)).end();
```

### Structured Error Response — Every Endpoint

```json
{
  "status":        400,
  "errorCode":     "VAL-002",
  "message":       "Required field is missing: payload.events",
  "correlationId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "timestamp":     "2025-12-18T10:00:00+05:30",
  "detail":        ["payload.events must not be empty"]
}
```

Fields: `status` mirrored in body · `errorCode` (18 typed codes) · `correlationId` for tracing · optional `detail` (violations or retry info).

---

## 5. Retry & Dead-Letter Architecture

### Three-Layer Resilience

```
Layer 1 — In-process (fast, synchronous)
  Kafka consumer: 3 retries × exponential backoff (1s→2s→4s)
  HTTP listener:  3 retries × exponential backoff → HTTP 503

Layer 2 — Retry queue (slow, async, cooling-off period)
  Poll interval = 5 minutes (the slow poll IS the back-off delay)
  Up to 3 re-entries into retry queue
  X-Error-Attempt header accumulated across all passes

Layer 3 — Final DLQ (permanent, manual investigation)
  Original body preserved unchanged — ready for replay
  Error context in Kafka record headers (8 standard headers)
```

### Routing Decision Table

| Exception | Main Consumer | Retry Consumer |
|-----------|:---:|:---:|
| `ValidationException`/`TransformationException` | DLQ | DLQ |
| `ExternalServiceException` (4xx) | DLQ | DLQ |
| `RetryQueueExhaustedException` | DLQ | DLQ |
| `RetryableException` (5xx/404/408/429/network) | Retry 3× → Retry Queue | Retry 3× → Retry Queue |
| `Exception` (unknown) | Retry 3× → Retry Queue | **DLQ** ← stricter in retry consumer |

**Best Practice — Invalid payloads never enter retry:**
```java
// configureRetryQueueErrorHandlers() — stricter than configureKafkaErrorHandlers()
// Exception catch-all → DLQ (not retry queue) in retry consumer
route.onException(Exception.class)
    .handled(true)
    .process(exchange -> setErrorProps(exchange, dlqTopic, "DLQ", serviceName))
    .to("direct:publishToDlq")
    .process(this::commitOffset)
.end();
```

### DLQ Message Contract

**Kafka body:** original raw message body, unchanged
**Kafka headers:** `X-Destination-Type`, `X-Correlation-ID`, `X-Service-Name`, `X-Error-Type`, `X-Error-Message`, `X-Error-Http-Status`, `X-Error-Attempt`, `X-Error-Timestamp`

**Replay:** re-publish DLQ body to source topic (`kraken-rcdc-events`) — no unwrapping needed.

---

## 6. Large Payload & File Handling

### Controls in Place

| Control | Value | Mechanism |
|---------|-------|-----------|
| File-size guard | 100 MB max | `ProfileReadsCsvProcessor.rejectIfOversized()` reads `CamelFileSize` / `CamelAwsS3ContentLength` header |
| Streaming CSV parse | ∞ rows | `BufferedReader` on raw `InputStream` — file never fully in heap |
| Bounded batch publish | 500 rows/batch | `ProfileReadsCsvProcessor.publishBatch()` via `ProducerTemplate` |
| Stream-caching spool | > 100 MB → disk | `camel.springboot.stream-caching-spool-threshold=104857600` |
| HTTP inbound cap | 2 MB (alarms), 1 MB (RCDC) | Kong `request-size-limiting` plugin |
| HTTP outbound cap | 1 MB | Kong `request-size-limiting` on SOA-RCDC and MDM proxy routes |
| FTP backlog control | 50 files/poll | `maxMessagesPerPoll=50` in `ftp.yml` |
| S3 backlog control | 50 files/poll | `maxMessagesPerPoll=50` in `s3.yml` |

**Best Practice — Streaming, not loading:**
```java
// ProfileReadsCsvProcessor — line-by-line streaming
try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
    String line;
    while ((line = reader.readLine()) != null) {
        // process one line, never accumulate the whole file
        if (currentBatch.size() >= kafkaBatchSize) {
            publishBatch(currentBatch, correlationId);  // flush memory
            currentBatch.clear();
        }
    }
}
```

---

## 7. Scalability Design

### Horizontal Scaling — Kafka Consumer Groups

```
Pod 1: RcdcRequestKafkaListner (group: kraken-cis-group)
Pod 2: RcdcRequestKafkaListner (group: kraken-cis-group)
Pod 3: RcdcRequestKafkaListner (group: kraken-cis-group)
         │                │                │
   Partition-0      Partition-1      Partition-2
         └────────── same Kafka topic ──────┘

Kafka assigns exactly one pod per partition.
No duplicate processing. No coordination needed.
```

**Best Practice — Manual offset commit prevents message loss on scale-out:**
```java
// BaseKafkaConsumerRoute — commit ONLY after successful handling
private void commitOffset(Exchange exchange) {
    KafkaManualCommit commit = exchange.getIn().getHeader(
        KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
    commit.commit();  // committed in success, DLQ, AND retry-queue paths
}
```

### KEDA Autoscaling — Kafka Lag Based

```yaml
# keda-scaledobject.yaml
triggers:
  - type: kafka
    metadata:
      consumerGroup: kraken-cis-group
      topic: kraken-rcdc-events
      lagThreshold: "100"    # add one pod per 100 unprocessed messages

minReplicaCount: 2   # always HA
maxReplicaCount: 10  # cap = Kafka partition count
```

### Vertical Scaling — Per-JVM Knobs

| Config | Default | Effect |
|--------|---------|--------|
| `kafka.consumer.concurrent-consumers` | 2 | Consumer threads per route per pod |
| `kafka.consumer.max-poll-records` | 500 | Records fetched per poll cycle |
| `ftp.profile-reads.thread-pool-size` | 4 | Parallel CSV file processing |
| `profile-reads.kafka-batch-size` | 500 | Rows per Kafka publish batch |

---

## 8. Gateway & Policy Enforcement

All inbound traffic passes through **Kong** (`docs/kong.yml`):

| Policy | Alarm Events | RCDC Command | HES Response | SOA-RCDC Proxy | MDM Proxy |
|--------|:---:|:---:|:---:|:---:|:---:|
| Rate limiting | 500/min | 200/min | 200/min | 300/min | 200/min |
| Request size | 2 MB | 1 MB | 1 MB | 1 MB | 1 MB |
| CORS | ✓ | — | ✓ | — | — |
| Correlation-ID inject | ✓ | ✓ | ✓ | ✓ | ✓ |
| Security headers | ✓ | ✓ | ✓ | — | — |

**Best Practice — Correlation ID enforced at the gateway:**
```yaml
# Kong injects X-Correlation-ID if caller omits it — never reaches the app without one
- name: correlation-id
  config:
    header_name: X-Correlation-ID
    generator: uuid
    echo_downstream: true
```

---

## 9. Observability & Tracing

### End-to-End Correlation ID

```
Client                Kong                 App                   Kafka               Downstream
  │                    │                    │                      │                      │
  │── POST /rcdc ──────►                    │                      │                      │
  │                    │── X-Correlation-ID:uuid ──►               │                      │
  │                    │                    │── MDC correlationId  │                      │
  │                    │                    │── PROP_CORRELATION_ID│                      │
  │                    │                    │── Kafka key=uuid ────►                      │
  │                    │                    │                      │── X-Correlation-ID ──►│
```

**Every log line** in the app carries the correlation ID via SLF4J MDC:
```json
{
  "timestamp": "2025-12-18T10:00:01.123+05:30",
  "level": "INFO",
  "correlationId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "routeId": "route-post-rcdc",
  "event": "routeCompleted",
  "durationMs": 45
}
```

### Spring Boot Actuator Endpoints (`:9091`)

| Endpoint | Use |
|----------|-----|
| `/actuator/health` | Liveness / readiness probe |
| `/actuator/camelroutes` | All route IDs, state, uptime |
| `/actuator/camelstats` | Exchange counts, failure rates |
| `/actuator/info` | Application version and metadata |

---

## 10. Best Practices Reference

### Code Quality

| Practice | Evidence |
|----------|----------|
| **No duplicate code** | `BaseRoute`, `BaseKafkaConsumerRoute`, `NetworkExceptionUtils` eliminate cross-cutting duplication |
| **Single responsibility** | Each processor does one thing: validate OR map OR call service |
| **Typed exceptions** | 6 exception types, 18 error codes — never generic `RuntimeException` in business logic |
| **Immutable models** | All models use Lombok `@Builder` + `@Data` — no setters in flow logic |
| **Constants over strings** | `LogConstants` centralises all Camel exchange property keys |

### Resilience

| Practice | Evidence |
|----------|----------|
| **Manual offset commit** | Offset committed ONLY after successful or error-routed processing |
| **Idempotent Kafka producer** | `acks=all` + `maxInFlightRequest=5` + `retries=3` |
| **Dead-letter on failure** | Every flow has a named DLQ topic; original body preserved for replay |
| **Never swallow exceptions** | `handled=false` on FTP/S3 errors — file moved then exception propagated |
| **Bounded memory** | `maxMessagesPerPoll=50`, `kafkaBatchSize=500`, stream-caching spool |

### Security

| Practice | Evidence |
|----------|----------|
| **Credentials externalised** | S3 keys in `s3.yml` (gitignored), FTP creds in `ftp.yml` |
| **Log injection prevention** | `MDCContextManager.sanitize()` strips control characters, caps at 128 chars |
| **CORS configured** | Kong CORS plugin per-endpoint, not wildcard on all routes |
| **Security response headers** | `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY` via Kong `response-transformer` |
| **Request size limits** | Kong rejects oversized payloads before they reach the app |

### Operational Excellence

| Practice | Evidence |
|----------|----------|
| **Environment profiles** | `dev` / `test` / `uat` / `prod` Spring profiles |
| **Externalised config** | All timeouts, topic names, batch sizes configurable without code change |
| **Structured JSON logging** | Logstash appender (uat/prod profiles) → ELK/Splunk ingestible |
| **API versioning** | `/api/v1` context path — breaking changes can be introduced under `/api/v2` |
| **Health probes** | K8s readiness + liveness on `/actuator/health` |
| **KEDA autoscaling** | Pods scale automatically based on Kafka consumer lag |

### Testing & Deployment

| Practice | Evidence |
|----------|----------|
| **Test dependencies ready** | WireMock 3.5.4 + Camel JUnit 5 in `pom.xml` — ready for integration tests |
| **Container ready** | Non-root `Dockerfile` (`appuser`), JRE 17 Alpine, reproducible build |
| **K8s manifests** | `deployment.yaml` (2 replicas, health probes) + `keda-scaledobject.yaml` |
| **Load simulation** | `scripts/load-simulation.sh` — 3-phase ramp test against all 3 endpoints |
| **Declarative gateway** | `docs/kong.yml` — DB-less Kong config, version-controlled |
| **API contract** | `docs/openapi.yaml` — OpenAPI 3.0 spec with full schemas and error shapes |

---

## Quick Reference — Topic Topology

```
Source Topics            Retry Topics                    DLQ Topics
─────────────────        ───────────────────────────     ──────────────────────────────
kraken-alarm-events      (no retry — fire-and-forget)   (no DLQ — 503 returned to caller)
kraken-rcdc-events       kraken-rcdc-retry-events        kraken-rcdc-dlq-events
kraken-rcdc-hes-*        kraken-rcdc-hes-retry-events    kraken-rcdc-hes-dlq-events
kraken-profile-reads-*   (no retry topic — file-level)   kraken-profile-reads-dlq-events
```

## Quick Reference — Port Layout

| Port | Owner | Use |
|------|-------|-----|
| `8000` | Kong | All inbound API traffic |
| `8080` | Camel Undertow | REST API (`/api/v1/...`) |
| `9091` | Spring Boot | Actuator (`/actuator/health`, etc.) |
| `9092` | Kafka | Broker |

---

*All code referenced in this document is in `src/main/java/com/pge/krakencis/`.*
