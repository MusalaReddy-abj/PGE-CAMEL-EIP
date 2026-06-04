# Kraken CIS Implementation — Technical Architecture

Apache Camel / Spring Boot integration service deployed on **Azure Kubernetes Service (AKS)**,
integrating PGE's Kraken CIS with downstream SOA services and AWS messaging/storage.

- Runtime: Spring Boot 3.2.5, Apache Camel 4.6, Java 17
- Messaging: AWS MSK (Kafka, SASL/IAM, port 9198)
- Storage / file transport: AWS S3 + FTP
- Outbound gateway: Kong (JWT HS256 bearer on every call)
- Observability: Micrometer → Prometheus, OpenTelemetry tracing (traceId/spanId in MDC)

---

## 1. System Context / Deployment (AKS + external systems)

```mermaid
flowchart TB
    subgraph clients["Clients / Upstream"]
        UP["SOA / Upstream systems<br/>(SOAP &amp; JSON over HTTPS)"]
        HES["HES (Trilliant)<br/>publishes RCDC status"]
        FILES["Profile Reads CSV<br/>(FTP / S3 drops)"]
    end

    subgraph aks["Azure Kubernetes Service (AKS) — kraken-cis namespace"]
        direction TB
        ING["Ingress / Service<br/>REST :8122 · Actuator :8121"]
        subgraph pod["Kraken CIS Pod(s)  (Spring Boot + Camel)"]
            REST["REST listeners<br/>/rcdc /alarms /odr /admin/dlq"]
            CONS["Kafka consumer routes<br/>(main + retry)"]
            POLL["FTP &amp; S3 pollers"]
            CORE["Camel engine<br/>processors · mappers · HttpClientService"]
        end
        ACT["Actuator<br/>/health /prometheus /metrics"]
    end

    subgraph kong["Kong API Gateway (k8s)"]
        KONG["Kong proxy<br/>+ JWT validation"]
    end

    subgraph soa["Downstream SOA Services"]
        RCDC_SVC["SOA-RCDC TargetService<br/>/rcdcrequest (JSON)"]
        MDM_SVC["SOA-MDM Service<br/>/rcdcswitchstatus (SOAP)"]
        ODR_SVC["SOA-ODR Mock<br/>/ondemandread (SOAP)"]
    end

    subgraph aws["AWS (ap-south-2)"]
        MSK["AWS MSK / Kafka<br/>3 brokers · SASL-IAM · :9198"]
        S3["AWS S3<br/>bucket: pge-int-demo"]
    end

    subgraph obs["Observability"]
        PROM["Prometheus"]
        OTEL["OTel Collector<br/>(optional)"]
    end

    FTPSRV["FTP server<br/>192.168.4.34:21"]

    UP -->|HTTPS SOAP/JSON| ING --> REST
    HES -->|publishes| MSK
    FILES --> FTPSRV
    FILES --> S3

    REST --> CORE
    CONS --> CORE
    POLL --> CORE
    FTPSRV <-->|poll / archive| POLL
    S3 <-->|poll / archive| POLL

    CORE -->|outbound HTTP + JWT| KONG
    KONG --> RCDC_SVC
    KONG --> MDM_SVC
    KONG --> ODR_SVC

    CORE <-->|produce / consume<br/>SASL-IAM| MSK
    CONS <--> MSK

    ACT -->|scrape| PROM
    CORE -.->|spans OTLP| OTEL
```

---

## 2. RCDC Request Flow (Connect / Disconnect command)

```mermaid
flowchart LR
    A["POST /api/v1/rcdc<br/>(SOAP XML)"] --> B["RcdcRequestHttpListner"]
    B --> C["SoapEnvelopeExtractor<br/>+ JAXB unmarshal"]
    C --> D["RcdcRequestProcessor<br/>validate · extract mRID/state"]
    D --> E["RcdcTargetMappingProcessor"]
    E --> P[("kraken-rcdc-events")]
    B --> ACK["Acknowledgement<br/>(SOAP 200)"]

    P --> K["RcdcRequestKafkaListner<br/>group: kraken-cis-group"]
    K --> TC["RcdcTargetCallProcessor<br/>→ SOARCDCRequestService"]
    TC --> HC["HttpClientService<br/>(3x retry, JWT)"]
    HC -->|via Kong| EXT["SOA-RCDC /rcdcrequest"]
    K --> CO["commit offset"]

    K -.->|RetryableException<br/>after 3 in-proc retries| RT[("kraken-rcdc-retry-events")]
    K -.->|Validation / 4xx| DLQ[("kraken-rcdc-dlq-events")]
    RT --> RC["RcdcRetryKafkaRoute<br/>5-min back-off poll"]
    RC -->|re-call| TC
    RC -.->|attempt &ge; 3| DLQ

    classDef topic fill:#fde68a,stroke:#b45309,color:#000;
    class P,RT,DLQ topic;
```

---

## 3. RCDC HES Response Flow (HES → MDM notification)

```mermaid
flowchart LR
    HES["HES (Trilliant)"] --> P[("kraken-rcdc-hes-response-events")]
    P --> K["RcdcResponseHesKafkaListner<br/>group: kraken-cis-group-mdm"]
    K --> MP["RcdcMdmNotificationProcessor"]
    MP --> MAP["RcdcMdmNotificationMapper<br/>→ CM-ChangeRCDSwitchStateResp SOAP"]
    MAP --> HC["HttpClientService (JWT)"]
    HC -->|via Kong| MDM["SOA-MDM /rcdcswitchstatus"]
    HC --> SF["SoapFaultInspector<br/>(fault in HTTP 200 → 5xx)"]
    K --> CO["commit offset"]

    K -.->|Retryable / SOAP fault| RT[("kraken-rcdc-hes-retry-events")]
    K -.->|Transformation / 4xx| DLQ[("kraken-rcdc-hes-dlq-events")]
    RT --> RC["RcdcHesRetryKafkaRoute<br/>5-min back-off poll"]
    RC -->|re-call MDM| MP
    RC -.->|attempt &ge; 3| DLQ

    classDef topic fill:#fde68a,stroke:#b45309,color:#000;
    class P,RT,DLQ topic;
```

---

## 4. Alarms Flow

```mermaid
flowchart LR
    A["POST /api/v1/alarms<br/>(JSON)"] --> L["AlarmHttpListner"]
    L --> EP["AlarmEventProcessor<br/>validate each event"]
    EP --> MAP["AlarmEventMapper<br/>→ KrakenEvent (messageId = UUID)"]
    MAP --> P[("kraken-alarm-events<br/>one msg per alarm")]
    L --> R["AlarmResponseProcessor<br/>HTTP 200"]
    classDef topic fill:#fde68a,stroke:#b45309,color:#000;
    class P topic;
```

---

## 5. Profile Reads Flow (FTP & S3 CSV ingestion)

```mermaid
flowchart TB
    subgraph src["Sources"]
        FTP["ProfileReadsFTPListner<br/>(ftp.listener.enabled)"]
        S3L["ProfileReadsS3Listner<br/>synchronous poll · CSV-only"]
    end
    FTP --> CSV
    S3L --> CSV
    CSV["ProfileReadsCsvProcessor<br/>validate header · stream rows<br/>batch 500 · async publish (≤4 in-flight)"]

    CSV --> EV[("kraken-profile-reads-events<br/>parsed rows")]
    CSV --> DLQR["direct:publishProfileReadsDlq"] --> DLQ[("kraken-profile-reads-dlq-events<br/>failed rows")]
    CSV --> AUDR["direct:publishProfileReadsAudit"] --> AUD[("kraken-profile-reads-audit-events<br/>SUCCESS / PARTIAL_FAILURE / CORRUPTED")]

    S3L -->|success| ARC["S3 Reads/Archive/"]
    S3L -->|error / non-CSV| ERR["S3 Reads/Error/"]
    FTP -->|success| ARCF["FTP Reads/Archive"]
    FTP -->|error| ERRF["FTP Reads/Error"]

    classDef topic fill:#fde68a,stroke:#b45309,color:#000;
    class EV,DLQ,AUD topic;
```

---

## 6. Retry / DLQ Mechanism (BaseKafkaConsumerRoute)

```mermaid
flowchart TB
    M["Main consumer<br/>processes message"] -->|success| OK["commit offset"]
    M -->|ValidationException<br/>TransformationException<br/>ExternalServiceException 4xx| DLQ[("DLQ topic<br/>immediate")]
    M -->|RetryableException / unknown| R3["3 in-process retries<br/>1s → 2s → 4s"]
    R3 -->|still failing| RT[("retry topic<br/>X-Error-Attempt = 1")]

    RT --> RQ["Retry consumer<br/>5-min back-off poll"]
    RQ -->|attempt &ge; max (3)| EXH["RetryQueueExhaustedException"] --> DLQ
    RQ -->|RetryableException| RT2[("retry topic<br/>attempt + 1")]
    RQ -->|any other error| DLQ
    RQ -->|success| OK2["commit offset"]

    DLQ --> MON["kraken-dlq-monitor group<br/>(never consumes)"]
    MON --> STATS["GET /api/v1/admin/dlq/stats<br/>lag → kafka.dlq.lag gauge"]

    classDef topic fill:#fde68a,stroke:#b45309,color:#000;
    class RT,RT2,DLQ topic;
```

Error context travels as Kafka **record headers** (no payload reformatting):
`X-Correlation-ID`, `X-Error-Type`, `X-Error-Message`, `X-Error-Http-Status`,
`X-Error-Attempt`, `X-Service-Name`, `X-Destination-Type`, `X-Error-Timestamp`.

---

## 7. Kafka Topics & Consumer Groups

| Topic | Producer | Consumer (group) |
|---|---|---|
| `kraken-rcdc-events` | RcdcRequestHttpListner | RcdcRequestKafkaListner (`kraken-cis-group`) |
| `kraken-rcdc-retry-events` | main consumer on failure | RcdcRetryKafkaRoute (`…-retry`) |
| `kraken-rcdc-dlq-events` | main + retry on exhaustion | `kraken-dlq-monitor` (stats only) |
| `kraken-rcdc-hes-response-events` | HES (external) | RcdcResponseHesKafkaListner (`…-mdm`) |
| `kraken-rcdc-hes-retry-events` | HES consumer on failure | RcdcHesRetryKafkaRoute (`…-hes-retry`) |
| `kraken-rcdc-hes-dlq-events` | HES main + retry | `kraken-dlq-monitor` |
| `kraken-alarm-events` | AlarmHttpListner | external |
| `kraken-profile-reads-events` | ProfileReadsCsvProcessor | external |
| `kraken-profile-reads-dlq-events` | ProfileReadsDlqRoute | `kraken-dlq-monitor` |
| `kraken-profile-reads-audit-events` | ProfileReadsAuditRoute | external |

---

## 8. Cross-Cutting Concerns

| Concern | Components |
|---|---|
| **Security** | `JwtTokenProvider` — HS256 bearer on every Kong-routed call; MSK SASL/IAM; S3 default-credentials (IAM role) |
| **Resilience** | `HttpClientService` (3× exponential retry); typed exceptions drive retry-topic vs DLQ routing |
| **Observability** | `StructuredLogger`, `RouteLoggingProcessor`, `MDCContextManager`; Micrometer → Prometheus; OpenTelemetry spans (traceId/spanId in MDC) |
| **Metrics** | `business.transaction.requests/events`, `kafka.consumed`, `kafka.dlq.published`, `kafka.retry.published`, `kafka.dlq.lag`, `http.outbound.*` |
| **Health** | `KafkaHealthIndicator` + Actuator `/health` |
| **Config** | `KafkaProducerConfig`, `ExternalServiceProperties`, `HttpClientProperties`, `ProfileReads{Ftp,S3}Properties`, `S3ClientConfig` |
```

