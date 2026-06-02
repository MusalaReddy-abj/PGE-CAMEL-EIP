# Failure Handling — Demonstration Guide

How the Kraken CIS Integration Platform handles every category of failure: partial failures,
downstream outages, message corruption, and SOAP faults.

---

## Overview — Failure Decision Tree

```
Failure occurs
  │
  ├─ HTTP Listener (inbound REST)
  │     ├─ Payload invalid         → HTTP 400 / 422  immediately (no retry)
  │     └─ Kafka/internal error    → retry 3× (1s→2s→4s) → HTTP 503 with retry count
  │
  ├─ Kafka Consumer (async downstream call)
  │     ├─ Payload corrupt         → DLQ immediately
  │     ├─ 4xx client error        → DLQ immediately
  │     ├─ 5xx / network / 404     → retry 3× → retry queue
  │     └─ Unknown exception       → retry 3× → retry queue
  │
  ├─ Retry-Queue Consumer
  │     ├─ Invalid payload         → DLQ immediately
  │     ├─ 4xx client error        → DLQ immediately
  │     ├─ Retry limit exceeded    → final DLQ
  │     ├─ 5xx / network           → retry 3× → retry queue (same topic)
  │     └─ Unknown exception       → DLQ immediately
  │
  ├─ SOAP Service
  │     ├─ soap:Client fault       → treated as 400 → DLQ immediately
  │     └─ soap:Server fault       → treated as 500 → retry 3× → retry queue
  │
  └─ CSV / File Ingestion (FTP / S3)
        ├─ File too large           → ValidationException → error dir / S3 Error/
        ├─ Header missing/corrupt   → ValidationException → error dir
        ├─ Individual row bad       → row → DLQ, file continues
        └─ All rows fail            → ValidationException → error dir
```

---

## 1. Partial Failures — Individual CSV Row Corruption

**Scenario:** A Profile Reads CSV file has 1,000 rows. Row 42 has a missing `mRID` field.

**Code location:** [`ProfileReadsCsvProcessor.java`](../src/main/java/com/pge/krakencis/processors/profilereads/ProfileReadsCsvProcessor.java)

```
profilereads_20251218.csv (1000 rows)
    │
    ├─ Row 1..41   → parsed OK  → batch published to kraken-profile-reads-events
    ├─ Row 42      → mRID missing → TransformationException caught
    │                               ProfileReadFailedRow built:
    │                               { fileName, lineNumber:42, rawLine, errorType,
    │                                 errorMessage, correlationId, timestamp }
    │                               added to PROP_FAILED_ROWS list
    ├─ Row 43..1000→ parsed OK  → batch published
    │
    ▼
direct:publishProfileReadsDlq
    └─ PROP_FAILED_ROWS (1 entry) → kraken-profile-reads-dlq-events
         body: { "fileName": "profilereads_20251218.csv",
                 "lineNumber": 42,
                 "rawLine": "CREATED,MeterReadings,,0.0.0.4...",
                 "errorType": "TransformationException",
                 "errorMessage": "Required field is missing: mRID" }
    │
    ▼
File → Reads/Archive/ (success — 999/1000 rows published)
```

**What the operator sees:**
- `kraken-profile-reads-events`: 999 messages
- `kraken-profile-reads-dlq-events`: 1 message with exact line number and raw content for replay

---

## 2. Complete File Corruption

**Scenario:** CSV file has a corrupted header row (missing columns).

```
corrupt_file.csv
    │
    ├─ Header: "Verb,Noun,timestamp,value"   ← missing mRID, ReadingType
    │
    ▼
profileReadsMapper.validateHeaders()
    └─ ValidationException: "missing required column(s): mrid, readingtype"
    │
    ▼
onException(Exception.class) → handled=false
    ├─ logFileError()   → logs fileName, errorType, errorMessage
    └─ moveToError()    → file → Reads/Error/corrupt_file.csv (S3)
                         file → .error/corrupt_file.csv        (FTP)
```

---

## 3. Downstream Service Unavailable — RCDC Flow

**Scenario:** SOA-RCDC service returns HTTP 503 (server restart in progress).

```
kraken-rcdc-events (Kafka)
    │
    ▼
RcdcRequestKafkaListner
    │
    ├─ Attempt 1: RcdcTargetCallProcessor → HTTP 503
    │              RcdcTargetResponseProcessor → RetryableException ("SOA-RCDC-TargetService returned HTTP 503")
    │              wait 1s
    │
    ├─ Attempt 2: HTTP 503 → RetryableException → wait 2s
    │
    ├─ Attempt 3: HTTP 503 → RetryableException → wait 4s
    │
    └─ All 3 retries exhausted
         │
         ▼
    onException(RetryableException.class) → KafkaErrorRoutes.prepareErrorMessage("RETRY")
         Body    = original RcdcTargetRequest JSON (unchanged, ready for replay)
         Headers = X-Destination-Type: RETRY
                   X-Error-Type: RetryableException
                   X-Error-Http-Status: 503
                   X-Error-Attempt: 3
                   X-Error-Timestamp: 2025-12-18T10:00:07+05:30
         │
         ▼
    kraken-rcdc-retry-events
```

**After 5 minutes** (KEDA retry consumer polls):

```
kraken-rcdc-retry-events
    │
    ▼
RcdcRetryKafkaRoute
    │
    ├─ parseAttempt() reads X-Error-Attempt header → 3
    ├─ 3 < maxRetryQueueAttempts(3)? YES → proceed
    │
    ├─ Attempt 1: HTTP 200 ✓  →  commitOffset()  →  DONE
    │   OR
    ├─ Attempt 1: still 503   → wait 1s
    ├─ Attempt 2: still 503   → wait 2s
    ├─ Attempt 3: still 503   → X-Error-Attempt: 3+3+1 = 7
    │              setErrorProps() → PROP_RETRY_ATTEMPT = 7
    │              → republished to kraken-rcdc-retry-events
    │
    └─ Third retry-queue pass: parseAttempt() → 7 >= 3 → RetryQueueExhaustedException
         │
         ▼
    onException(RetryQueueExhaustedException.class) → DLQ
         Body    = original message (unchanged)
         Headers = X-Destination-Type: DLQ
                   X-Error-Attempt: 10
         │
         ▼
    kraken-rcdc-dlq-events  ← manual investigation required
```

---

## 4. Non-Retryable Client Error — RCDC Flow

**Scenario:** SOA-RCDC returns HTTP 400 (malformed request body).

```
kraken-rcdc-events
    │
    ▼
RcdcTargetResponseProcessor
    └─ status = 400 → ExternalServiceException.httpError()
         (not 5xx / 404 / 408 / 429 → not retryable)
    │
    ▼
onException(ExternalServiceException.class) → DLQ immediately
    Body    = original message
    Headers = X-Destination-Type: DLQ
              X-Error-Type: ExternalServiceException
              X-Error-Http-Status: 400
              X-Error-Message: "SOA-RCDC-TargetService returned HTTP 400"
    │
    ▼
kraken-rcdc-dlq-events  ← no retry — fix the payload and replay manually
```

---

## 5. SOAP Fault Handling — MDM Notification

**Scenario A:** MDM SOAP service returns HTTP 200 with `<soap:Server>` fault (transient server error).

```
SOAMDMNotificationService.sendNotification()
    └─ response: HTTP 200
                 body: "<soap:Fault><faultcode>soap:Server</faultcode>
                         <faultstring>Internal server error</faultstring></soap:Fault>"

SoapFaultInspector.hasFault()          → true
SoapFaultInspector.isClientFault()     → false (soap:Server ≠ client)
SoapFaultInspector.inferHttpStatus()   → 500 (virtual server error)

RcdcMdmNotificationProcessor.callMdmService()
    └─ status = 500 (inferred) → RetryableException("SOA-MDM-Service returned HTTP 500")
    │
    ▼
Retry 3× → retry queue → (same as downstream outage flow above)
```

**Scenario B:** MDM SOAP returns `<soap:Client>` fault (bad request — mapping error).

```
SoapFaultInspector.isClientFault()     → true
SoapFaultInspector.inferHttpStatus()   → 400 (virtual client error)
SoapFaultInspector.extractFaultMessage()→ "Invalid mRID format"

RcdcMdmNotificationProcessor.callMdmService()
    └─ status = 400 (inferred) → ExternalServiceException.httpError()
    │
    ▼
DLQ immediately — no retry
```

**Code location:** [`SoapFaultInspector.java`](../src/main/java/com/pge/krakencis/services/SoapFaultInspector.java)

---

## 6. Inbound HTTP Payload Validation Failure

**Scenario:** Kraken CIS sends an alarm event with missing `deviceIdentifierNumber`.

```
POST /api/v1/alarms
{
  "header": { "verb": "POST", "noun": "Alarm" },
  "payload": {
    "events": [ { "category": "POWER_OUTAGE", "createdDateTime": "2025-12-18T10:00:00Z" } ]
  }
}

AlarmEventProcessor.validateEvent()
    └─ deviceIdentifierNumber == null → ValidationException.missingField("deviceIdentifierNumber")

BaseRoute.onException(ValidationException.class)
    └─ handled=true → HTTP 400 immediately — NO RETRY

Response:
HTTP 400
{
  "status": 400,
  "errorCode": "VAL-002",
  "message": "Required field is missing: deviceIdentifierNumber",
  "correlationId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "timestamp": "2025-12-18T10:00:00+05:30"
}
```

---

## 7. Transient Kafka Unavailability — HTTP Listener

**Scenario:** Kafka broker is unreachable when an alarm batch arrives.

```
POST /api/v1/alarms (valid payload)
    │
    ▼
AlarmEventProcessor → List<KrakenEvent>  ✓
    │
    ▼
KafkaRoute (direct:publishToKafka)
    └─ broker 192.168.4.34:9092 unreachable → Exception

BaseRoute.onException(Exception.class)
    .maximumRedeliveries(3)
    .redeliveryDelay(1_000)
    .backOffMultiplier(2)

    Attempt 1: broker down → wait 1s
    Attempt 2: broker down → wait 2s
    Attempt 3: broker down → wait 4s
    Retries exhausted
    │
    ▼
RouteExceptionProcessor.systemWithRetryInfo()

Response:
HTTP 503
{
  "status": 503,
  "errorCode": "SYS-001",
  "message": "Service temporarily unavailable after 3 retry attempt(s)",
  "correlationId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "timestamp": "2025-12-18T10:00:07+05:30",
  "detail": {
    "retriesAttempted": 3,
    "cause": "Connection refused: 192.168.4.34:9092"
  }
}

Caller should retry after back-off.
```

---

## 8. File Size Guard — Large File Rejection

**Scenario:** A CSV file of 500 MB arrives on FTP / S3 (normal files are < 5 MB).

```
FTP consumer sets Exchange.FILE_SIZE = 524288000 (500 MB)
S3 consumer sets CamelAwsS3ContentLength = 524288000

ProfileReadsCsvProcessor.rejectIfOversized()
    └─ 524288000 > maxFileSizeMb(100) × 1024 × 1024
       → ValidationException: "file size must be ≤ 100 MB (got 500 MB)"
    │
    ▼
FTP: onException(Exception.class).handled(false)
    → Camel FTP moveFailed → .error/large_file.csv

S3:  onException(Exception.class).handled(false)
    → moveToError() → s3://pge-int-demo/Reads/Error/large_file.csv
    → deleteObject(original)
```

---

## 9. Error Response Reference

All HTTP endpoints return the same `ErrorResponse` structure:

| HTTP Status | `errorCode` | When | Retry? |
|-------------|------------|------|--------|
| `400` | `VAL-002` | Missing required field | No |
| `400` | `VAL-003` | Invalid field format (e.g. wrong state value) | No |
| `422` | `TRF-001` | Transformation/mapping failure | No |
| `422` | `TRF-003` | Invalid date/timestamp format | No |
| `503` | `SYS-001` | Transient failure after 3 retries | Yes — retry after delay |
| `500` | `SYS-001` | Unexpected internal error | No |

---

## 10. DLQ Message Structure

Every message in a DLQ topic (`kraken-*-dlq-events`) has:

**Kafka record body** — original message, unchanged, ready for replay:
```json
{ "verb": "Create", "noun": "RCDSwitchState", ... }
```

**Kafka record headers** — error context:
```
X-Destination-Type  : DLQ
X-Correlation-ID    : 3fa85f64-5717-4562-b3fc-2c963f66afa6
X-Service-Name      : SOA-RCDC-TargetService
X-Error-Type        : ExternalServiceException
X-Error-Message     : SOA-RCDC-TargetService returned HTTP 400
X-Error-Http-Status : 400
X-Error-Attempt     : 4
X-Error-Timestamp   : 2025-12-18T10:05:00+05:30
```

**Replay:** Publish the DLQ message body back to the source topic
(`kraken-rcdc-events`) with a corrected payload.

---

## 11. Failure Handling Code Locations

| Scenario | Class | Method |
|----------|-------|--------|
| HTTP payload validation | [`RouteExceptionProcessor`](../src/main/java/com/pge/krakencis/processors/RouteExceptionProcessor.java) | `validation()` |
| HTTP retry + 503 | [`BaseRoute`](../src/main/java/com/pge/krakencis/routes/BaseRoute.java) | `processingRoute()` |
| Kafka consumer retry → retry queue | [`BaseKafkaConsumerRoute`](../src/main/java/com/pge/krakencis/routes/BaseKafkaConsumerRoute.java) | `configureKafkaErrorHandlers()` |
| Retry queue → final DLQ | [`BaseKafkaConsumerRoute`](../src/main/java/com/pge/krakencis/routes/BaseKafkaConsumerRoute.java) | `configureRetryQueueErrorHandlers()` |
| DLQ / retry message format | [`KafkaErrorRoutes`](../src/main/java/com/pge/krakencis/routes/KafkaErrorRoutes.java) | `prepareErrorMessage()` |
| SOAP fault detection | [`SoapFaultInspector`](../src/main/java/com/pge/krakencis/services/SoapFaultInspector.java) | `inferHttpStatus()` |
| Per-row CSV failure | [`ProfileReadsCsvProcessor`](../src/main/java/com/pge/krakencis/processors/profilereads/ProfileReadsCsvProcessor.java) | `process()` |
| CSV row DLQ | [`ProfileReadsDlqRoute`](../src/main/java/com/pge/krakencis/routes/profilereads/ProfileReadsDlqRoute.java) | `configure()` |
| File-size guard | [`ProfileReadsCsvProcessor`](../src/main/java/com/pge/krakencis/processors/profilereads/ProfileReadsCsvProcessor.java) | `rejectIfOversized()` |
| Network error → RetryableException | [`RcdcTargetCallProcessor`](../src/main/java/com/pge/krakencis/processors/rcdc/request/RcdcTargetCallProcessor.java) | `process()` |
| HTTP status classification | [`RcdcTargetResponseProcessor`](../src/main/java/com/pge/krakencis/processors/rcdc/request/RcdcTargetResponseProcessor.java) | `process()` |
| Retry queue consumer | [`RcdcRetryKafkaRoute`](../src/main/java/com/pge/krakencis/routes/rcdc/request/RcdcRetryKafkaRoute.java) | `configure()` |
| Attempt counter accumulation | [`BaseKafkaConsumerRoute`](../src/main/java/com/pge/krakencis/routes/BaseKafkaConsumerRoute.java) | `setErrorProps()` |
