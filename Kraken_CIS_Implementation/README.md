# Kraken CIS Implementation

Apache Camel-based enterprise integration layer between PGE's **Kraken CIS** platform and downstream operational systems (SOA-RCDC, HES, MDM).

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Message Flows](#message-flows)
4. [Project Structure](#project-structure)
5. [Prerequisites](#prerequisites)
6. [Configuration](#configuration)
7. [Running Locally](#running-locally)
8. [Docker](#docker)
9. [Kubernetes](#kubernetes)
10. [API Endpoints](#api-endpoints)
11. [Logging](#logging)
12. [Testing](#testing)
13. [Exception Handling](#exception-handling)

---

## Overview

This service acts as the integration bus for PGE's Customer Information System (CIS). It receives commands and events from the Kraken CIS platform via Kafka, orchestrates calls to downstream SOA/HES/MDM services, and publishes results back to Kafka topics for other consumers.

**Key capabilities:**

| Capability | Technology |
|---|---|
| Route orchestration | Apache Camel 4.6 |
| REST inbound listener | Camel Undertow |
| Kafka inbound / outbound | Camel Kafka (manual offset commit) |
| HTTP outbound calls | Spring `RestClient` via `HttpClientService` |
| SOAP outbound calls | HTTP with `text/xml` content type |
| Structured logging | Logback + Logstash JSON encoder |
| Metrics | Micrometer + Spring Boot Actuator |

---

## Architecture

```
                     ┌───────────────────────────────────────────────────┐
                     │            Kraken CIS Implementation               │
                     │                                                     │
  Kafka              │  ┌──────────────────────┐   ┌───────────────────┐ │
  kraken-rcdc-events─┼─►│ RcdcRequestKafka     │──►│ SOARCDCRequest    │─┼─► SOA-RCDC (HTTP)
                     │  │ Listner              │   │ Service           │ │
                     │  └──────────────────────┘   └───────────────────┘ │
                     │                                                     │
  HTTP POST          │  ┌──────────────────────┐   ┌───────────────────┐ │
  /api/v1/rcdc/resp ─┼─►│ RcdcHesResponse      │──►│ Kafka publish     │─┼─► kraken-rcdc-hes-
                     │  │ HttpListner          │   │                   │ │    response-events
                     │  └──────────────────────┘   └───────────────────┘ │
                     │                                                     │
  Kafka              │  ┌──────────────────────┐   ┌───────────────────┐ │
  rcdc-hes-response ─┼─►│ RcdcResponseHes      │──►│ SOAMDMNotification│─┼─► MDM (SOAP)
  -events            │  │ KafkaListner         │   │ Service           │ │
                     │  └──────────────────────┘   └───────────────────┘ │
                     │                                                     │
  HTTP POST          │  ┌──────────────────────┐   ┌───────────────────┐ │
  /api/v1/alarms ────┼─►│ AlarmEvent           │──►│ Kafka publish     │─┼─► kraken-alarm-events
                     │  │ Processor            │   │                   │ │
                     │  └──────────────────────┘   └───────────────────┘ │
                     └───────────────────────────────────────────────────┘
```

### Cross-cutting concerns

Every route that uses `BaseRoute.processingRoute()` or the Kafka listener template automatically gets:

- **Correlation ID** — seeded from the inbound `X-Correlation-ID` HTTP header or Kafka message key. A UUID is generated if neither is present. The ID is propagated to all log lines, outbound headers, and Kafka message keys.
- **Structured audit logging** — route entry/exit timestamps and durations are written to a dedicated `AUDIT` log appender.
- **Standardised exception handling** — `ValidationException` → HTTP 400, `TransformationException` → 422, all other domain exceptions → 500, unexpected exceptions → 500 with code `SYS-001`.
- **Kafka manual offset commit** — both Kafka consumer routes disable auto-commit and call `KafkaManualCommit.commit()` inside a `doFinally` block, ensuring the offset is always advanced (even after an error) to prevent infinite poison-pill redelivery.

---

## Message Flows

### 1. RCDC Request — Remote Connect / Disconnect Command

```
Kafka[kraken-rcdc-events]
  │
  ▼ RcdcRequestKafkaListner
     extractCorrelationId       — reads Kafka message key; generates UUID if absent
     routeLoggingProcessor      — MDC population, audit entry log
     doTry
       RcdcTargetCallProcessor
         └─ SOARCDCRequestService.sendCommand()  ──► SOA-RCDC HTTP POST
       RcdcTargetResponseProcessor
         └─ throws ExternalServiceException on non-2xx response
     doCatch(Exception)
       exceptionProcessor.system()               — logs failure
     doFinally
       commitOffset()                            — always commits Kafka offset
       routeLoggingProcessor.exit()              — audit exit log, clears MDC
```

**Inbound topic:** `kafka.topic.rcdc` (default: `kraken-rcdc-events`)  
**Consumer group:** `kafka.consumer.group-id` (default: `kraken-cis-group`)

---

### 2. RCDC HES Response — Inbound HTTP to Kafka

```
HTTP POST /api/v1/rcdc/response
  │
  ▼ RcdcHesResponseHttpListner  (extends BaseRoute)
     [BaseRoute] CorrelationIdProcessor
     [BaseRoute] routeLoggingProcessor.entry
     RcdcHesResponseProcessor    — validates and extracts correlationId + mRID
     publish → Kafka[kraken-rcdc-hes-response-events]
     RcdcHesAckProcessor         — builds HTTP 202 acknowledgement body
     [BaseRoute] routeLoggingProcessor.exit
```

**REST endpoint:** `POST /api/v1/rcdc/response`  
**Outbound topic:** `kafka.topic.rcdc-hes-response` (default: `kraken-rcdc-hes-response-events`)

---

### 3. RCDC HES Response → MDM Notification — Kafka to SOAP

```
Kafka[kraken-rcdc-hes-response-events]
  │
  ▼ RcdcResponseHesKafkaListner
     extractCorrelationId
     routeLoggingProcessor.entry
     doTry
       RcdcMdmNotificationProcessor
         └─ SOAMDMNotificationService.notify()  ──► MDM SOAP endpoint
     doCatch(Exception)
       exceptionProcessor.system()
     doFinally
       commitOffset()
       routeLoggingProcessor.exit()
```

**Inbound topic:** `kafka.topic.rcdc-hes-response` (default: `kraken-rcdc-hes-response-events`)  
**Consumer group:** `{kafka.consumer.group-id}-mdm`

---

### 4. Alarm Events — Inbound HTTP to Kafka

```
HTTP POST /api/v1/alarms
  │
  ▼ (alarm route, extends BaseRoute)
     [BaseRoute] CorrelationIdProcessor
     AlarmEventProcessor   — validates AlarmRequest; maps to List<KrakenEvent>
     KafkaMessageProcessor — sets Kafka message key from event.messageId
     publish → Kafka[kraken-alarm-events-{profile}]
```

---

## Project Structure

```
src/main/java/com/pge/krakencis/
├── KrakenCisApplication.java            Spring Boot entry point
│
├── configs/
│   ├── CamelConfig.java                 Stream caching + Jackson type converter
│   ├── ExternalServiceProperties.java   Per-environment URL/timeout/content-type registry
│   └── KafkaProducerConfig.java         Kafka producer URI builder with input validation
│
├── exceptions/
│   ├── KrakenBaseException.java         Abstract root with errorCode, correlationId, context map
│   ├── ValidationException.java         VAL-xxx  — invalid or missing input  (→ HTTP 400)
│   ├── TransformationException.java     TRF-xxx  — mapping / serialization failures (→ HTTP 422)
│   ├── ExternalServiceException.java    EXT-xxx  — downstream HTTP errors
│   ├── RetryableException.java          RET-xxx  — transient errors eligible for Camel retry
│   ├── SystemException.java             SYS-xxx  — unexpected internal errors (→ HTTP 500)
│   ├── IntegrationException.java        INT-xxx  — route / message processing failures
│   ├── ErrorCode.java                   Enum of all error codes (VAL/INT/EXT/TRF/RET/SYS)
│   └── ErrorCategory.java              Enum: VALIDATION, INTEGRATION, EXTERNAL_SERVICE, …
│
├── logging/
│   ├── StructuredLogger.java            Thin SLF4J wrapper for key=value structured log lines
│   ├── LogConstants.java                MDC keys, exchange property keys, status constants
│   ├── MDCContextManager.java           Correlation ID seeding, sanitisation, MDC lifecycle
│   ├── RouteLoggingProcessor.java       Entry/exit audit Processors consumed by BaseRoute
│   └── AuditLogger.java                Domain-level audit helpers (routeStart, kafkaPublish, …)
│
├── models/
│   ├── KrakenEvent.java                 Canonical outbound event model published to Kafka
│   ├── alarms/                          AlarmRequest, AlarmHeader, AlarmPayload, AlarmEvent, …
│   └── rcdc/
│       ├── request/                     RcdcRequest, RcdcHeader, RcdcPayload, RcdcState, …
│       │   └── target/                 RcdcTargetRequest — mapped model for the SOA service
│       └── response/                    RcdcHesResponseMessage and nested response types
│
├── mappers/
│   ├── alarms/AlarmEventMapper.java     AlarmEvent  → KrakenEvent
│   └── rcdc/
│       ├── request/RcdcMapper.java      RcdcRequest → RcdcTargetRequest
│       └── response/RcdcMdmNotificationMapper.java
│
├── processors/
│   ├── BaseProcessor.java               Abstract Camel Processor providing a shared logger
│   ├── CorrelationIdProcessor.java      Seeds X-Correlation-ID from header or UUID
│   ├── RouteExceptionProcessor.java     Typed exception → structured JSON error response
│   ├── KafkaPublishProcessor.java       Sets Kafka message key and logs publish attempts
│   ├── KafkaMessageProcessor.java       Prepares a single KrakenEvent for Kafka publishing
│   ├── alarms/
│   │   ├── AlarmEventProcessor.java     Validates + maps AlarmRequest → KrakenEvent list
│   │   └── AlarmResponseProcessor.java
│   └── rcdc/
│       ├── request/
│       │   ├── RcdcRequestProcessor.java        Validates inbound RcdcRequest
│       │   ├── RcdcTargetMappingProcessor.java  Maps request to RcdcTargetRequest
│       │   ├── RcdcTargetCallProcessor.java     Delegates HTTP call to SOARCDCRequestService
│       │   └── RcdcTargetResponseProcessor.java Throws ExternalServiceException on non-2xx
│       └── response/
│           ├── RcdcHesResponseProcessor.java    Validates and extracts HES response data
│           └── RcdcHesAckProcessor.java         Builds HTTP 202 acknowledgement
│
├── routes/
│   ├── BaseRoute.java                   Abstract base; constructor-injected cross-cutting processors
│   ├── RestApiConfig.java               Undertow REST DSL (host, port, CORS, OpenAPI path)
│   └── rcdc/
│       ├── request/RcdcRequestKafkaListner.java     Kafka consumer → SOA-RCDC HTTP
│       └── response/
│           ├── RcdcHesResponseHttpListner.java      HTTP listener → Kafka publish
│           └── RcdcResponseHesKafkaListner.java     Kafka consumer → MDM SOAP
│
└── services/
    ├── HttpClientService.java             Spring RestClient wrapper for all outbound HTTP
    ├── HttpOutboundRequest.java           Protocol-agnostic request descriptor (REST + SOAP)
    ├── HttpOutboundResponse.java          Response envelope with isSuccess() / isClientError()
    ├── SOARCDCRequestService.java         Sends RCDC commands to the SOA target service
    └── SOAMDMNotificationService.java     Sends MDM notifications via SOAP

config/
├── dev/    application-dev.yml, kafka.yml
├── test/   application-test.yml, kafka.yml
├── uat/    application-uat.yml, kafka.yml
└── prod/   application-prod.yml, kafka.yml   (secrets via env vars — no passwords in files)

deployment/
├── docker/     Dockerfile (eclipse-temurin:17-jre-alpine), docker-compose.yml
└── kubernetes/ deployment.yaml  (2 replicas + ClusterIP Service + health probes)
```

---

## Prerequisites

| Requirement | Version |
|---|---|
| Java | 17 |
| Maven | 3.8+ |
| Apache Kafka | 3.x |
| PostgreSQL | 14+ (only if SQL routes are enabled) |

---

## Configuration

Select a profile at startup:

```bash
-Dspring.profiles.active=dev    # dev | test | uat | prod
```

### Core properties

| Property | Default | Description |
|---|---|---|
| `rest.host` | `0.0.0.0` | REST server bind address |
| `rest.port` | `8080` | REST server port |
| `kafka.producer.brokers` | `localhost:9092` | Kafka broker list |
| `kafka.consumer.group-id` | `kraken-cis-group` | Base consumer group |
| `kafka.consumer.auto-offset-reset` | `earliest` | Offset reset policy |
| `kafka.consumer.max-poll-records` | `500` | Max records per poll |
| `kafka.topic.rcdc` | `kraken-rcdc-events` | RCDC command topic |
| `kafka.topic.rcdc-hes-response` | `kraken-rcdc-hes-response-events` | HES response topic |

### External service endpoints

Configured under `external-services.services.<key>` in the active profile's `application-{profile}.yml`:

```yaml
external-services:
  services:
    rcdc:
      url: http://soa-host/api/rcdc/command
      timeout: 30000           # milliseconds
      content-type: application/json
    rcdc-mdm-notification:
      url: http://mdm-host/ws/RCDSwitchState
      timeout: 30000
      content-type: "text/xml; charset=utf-8"
```

The `ExternalServiceProperties` bean validates that each required key exists at startup, providing a clear error message if a URL is missing rather than failing silently at runtime.

### Production secrets

Production credentials must be supplied as environment variables — never hard-coded:

| Variable | Purpose |
|---|---|
| `DB_USERNAME` / `DB_PASSWORD` | PostgreSQL credentials |
| `RCDC_TARGET_URL` | SOA-RCDC endpoint URL |
| `MDM_SOAP_URL` | MDM SOAP endpoint URL |
| `REST_PORT` | Override REST bind port |

---

## Running Locally

```bash
# Build
mvn clean package -DskipTests

# Run (dev profile — connects to localhost Kafka and dev databases)
java -Dspring.profiles.active=dev \
     -jar target/kraken-cis-implementation-*.jar
```

| Endpoint | URL |
|---|---|
| REST API | `http://localhost:8080/api/v1` |
| OpenAPI docs | `http://localhost:8080/api-doc` |
| Health check | `http://localhost:8080/actuator/health` |
| Metrics | `http://localhost:8080/actuator/metrics` |

---

## Docker

```bash
# Build the image (requires the fat JAR to exist in target/)
mvn clean package -DskipTests
docker build -t kraken-cis-implementation:latest deployment/docker/

# Run a single container (dev profile)
docker run -p 8080:8080 \
  -e SPRING_PROFILES_ACTIVE=dev \
  kraken-cis-implementation:latest

# Run the full local stack (app + ActiveMQ)
docker-compose -f deployment/docker/docker-compose.yml up
```

---

## Kubernetes

```bash
# Apply manifests to the current cluster context
kubectl apply -f deployment/kubernetes/deployment.yaml

# Monitor rollout
kubectl rollout status deployment/kraken-cis-implementation

# List pods
kubectl get pods -l app=kraken-cis
```

The `Deployment` runs 2 replicas. Readiness and liveness probes target `/actuator/health` on port 8080. The `ClusterIP` Service exposes the REST API on port 80 internally.

---

## API Endpoints

| Method | Path | Description | Content-Type |
|---|---|---|---|
| `POST` | `/api/v1/rcdc/response` | Receive RCDC response from HES; publishes to Kafka | `application/json` |
| `POST` | `/api/v1/alarms` | Ingest alarm events from Kraken; publishes to Kafka | `application/json` |
| `GET` | `/api-doc` | OpenAPI 3.0 specification | `application/json` |
| `GET` | `/actuator/health` | Spring Boot health indicator | `application/json` |
| `GET` | `/actuator/metrics` | Micrometer metrics | `application/json` |

Pass `X-Correlation-ID: <uuid>` on any inbound request to have the same ID propagated through all log lines and downstream calls. A UUID is generated automatically if the header is absent or blank.

---

## Logging

Three log files are produced at runtime (paths relative to the working directory):

| File | Format | Profiles | Retention |
|---|---|---|---|
| `logs/kraken-cis.log` | Plain text | dev, test | 30 days / 3 GB |
| `logs/kraken-cis-json.log` | Logstash JSON | uat, prod | 30 days |
| `logs/kraken-cis-audit.log` | Logstash JSON | all | 90 days |

### MDC fields included in every log line

| Field | Description |
|---|---|
| `correlationId` | End-to-end trace ID (sanitised — control chars stripped, max 128 chars) |
| `routeId` | Camel route ID (e.g. `route-rcdc-kafka-consumer`) |
| `exchangeId` | Camel exchange ID |
| `messageType` | Logical message type |
| `sourceSystem` | Originating system |
| `targetSystem` | Downstream system being called |

The `AUDIT` logger writes to `kraken-cis-audit.log` on all profiles. Audit events include: route start/end with duration, Kafka publishes, and external service calls.

---

## Testing

```bash
# Run all tests
mvn test

# Run with the test profile
mvn test -Dspring.profiles.active=test
```

Test dependencies: `spring-boot-starter-test` and `camel-test-spring-junit5`.

Route tests should use `@CamelSpringBootTest` and stub external service beans with `@MockBean`.

---

## Exception Handling

All domain exceptions extend `KrakenBaseException` and carry:
- a typed `ErrorCode` (e.g. `EXT-003`)
- an `ErrorCategory` enum value
- the `correlationId` for traceability
- an optional `context` map for structured diagnostic data

| Exception | HTTP | Code prefix | Typical cause |
|---|---|---|---|
| `ValidationException` | 400 | `VAL-` | Missing or malformed request field |
| `TransformationException` | 422 | `TRF-` | Model mapping or serialisation failure |
| `ExternalServiceException` | 500 | `EXT-` | Downstream service returned non-2xx |
| `RetryableException` | — | `RET-` | Transient error; Camel retry policy applies |
| `IntegrationException` | 500 | `INT-` | Route or message processing failure |
| `SystemException` | 500 | `SYS-` | Unexpected internal error |
