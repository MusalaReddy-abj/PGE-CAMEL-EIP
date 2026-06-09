# Kraken CIS Integration Guide
### PGE — Apache Camel EIP Platform
**Version:** 1.0  
**Date:** June 2026  
**Application:** kraken-cis-implementation  

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Infrastructure & Configuration](#2-infrastructure--configuration)
3. [Use Case 1 — RCDC (Remote Connect / Disconnect Command)](#3-use-case-1--rcdc-remote-connect--disconnect-command)
4. [Use Case 2 — OnDemandRead (ODR)](#4-use-case-2--ondemandread-odr)
5. [Use Case 3 — Alarm Events](#5-use-case-3--alarm-events)
6. [Use Case 4 — Profile Reads (FTP / S3)](#6-use-case-4--profile-reads-ftp--s3)
7. [SOAP Envelope Handling](#7-soap-envelope-handling)
8. [Retry & Error Policy](#8-retry--error-policy)
9. [Kafka Topics Reference](#9-kafka-topics-reference)
10. [External Service Endpoints](#10-external-service-endpoints)
11. [Observability & Monitoring](#11-observability--monitoring)
12. [Distributed Tracing & Logging](#12-distributed-tracing--logging)

---

## 1. System Overview

Kraken CIS is an Apache Camel 4.6 based integration middleware running on Spring Boot 3.2. It acts as a reliable, fault-tolerant integration hub between upstream systems (CIS, HES) and downstream services (SOA-RCDC, MDM, Kafka consumers).

### High-Level Architecture

```
                        ┌─────────────────────────────────────────────┐
                        │          Kraken CIS  (Port 8122)             │
                        │                                               │
  CIS / HES ──SOAP──►  │  POST /api/v1/rcdc          RCDC Command     │──JSON──► SOA-RCDC
  CIS / HES ──JSON──►  │  POST /api/v1/rcdc/response  HES Callback    │──SOAP──► MDM Service
  CIS / HES ──SOAP──►  │  POST /api/v1/odr            OnDemandRead    │──SOAP──► ODR Mock
  CIS / HES ──JSON──►  │  POST /api/v1/alarms         Alarm Events    │──────►  Kafka
  FTP / S3  ──CSV──►   │  File Poller                 Profile Reads   │──────►  Kafka
                        └──────────────────────────────────────────────┘
                                            │
                                     Kafka Broker
                                  192.168.4.144:9092
                                            │
                           ┌────────────────┼─────────────────┐
                      RCDC Consumer    HES Consumer     Profile Consumer
```

### Technology Stack

| Component        | Technology              | Version |
|------------------|-------------------------|---------|
| Integration      | Apache Camel            | 4.6.0   |
| Framework        | Spring Boot             | 3.2.5   |
| REST Server      | Camel Undertow          | 4.6.0   |
| Messaging        | Apache Kafka            | —       |
| XML Binding      | JAXB (Jakarta)          | —       |
| Tracing          | Micrometer + OTel       | —       |
| Metrics          | Micrometer + Prometheus | —       |
| Logging          | Logstash JSON Encoder   | 7.4     |
| Java             | OpenJDK                 | 17      |

---

## 2. Infrastructure & Configuration

### Ports

| Port | Purpose |
|------|---------|
| `8122` | Camel REST API — all business endpoints |
| `8121` | Spring Boot Actuator — health, metrics, management |

### Key Service URLs

| Service | URL |
|---------|-----|
| SOA-RCDC Target | `http://192.168.4.34:9000/rcdcrequest` |
| MDM SOAP Notification | `http://192.168.4.34:9000/rcdcswitchstatus` |
| ODR Mock SOAP | `http://192.168.4.34:9000/ondemandread` |
| Kafka Broker | `192.168.4.144:9092` |
| FTP Server | `192.168.4.34:21` |

### Kafka Consumer Groups

| Consumer Group | Topic Consumed | Purpose |
|----------------|---------------|---------|
| `kraken-cis-group` | `kraken-rcdc-events` | Main RCDC command processing |
| `kraken-cis-group-mdm` | `kraken-rcdc-hes-response-events` | MDM SOAP notification |
| `kraken-cis-group-retry` | `kraken-rcdc-retry-events` | RCDC retry queue (5 min poll) |
| `kraken-cis-group-hes-retry` | `kraken-rcdc-hes-retry-events` | MDM retry queue (5 min poll) |

---

## 3. Use Case 1 — RCDC (Remote Connect / Disconnect Command)

### Business Purpose

CIS instructs Kraken to connect or disconnect a meter. Kraken routes the command asynchronously to the HES via SOA-RCDC, then notifies MDM when the switch execution result is received back from HES.

This is a **two-leg asynchronous flow** decoupled via Kafka.

---

### Complete Flow Diagram

```
  CIS
   │
   │  POST /api/v1/rcdc  (SOAP XML)
   ▼
┌─────────────────────────────┐
│  RcdcRequestHttpListner     │
│  1. Strip SOAP envelope      │
│  2. JAXB unmarshal           │
│  3. Validate correlationID,  │
│     mRID, state              │
│  4. Map → JSON               │
│  5. Publish to Kafka         │
│  6. Return SOAP ACK          │
└─────────────────────────────┘
   │                    │
   │ HTTP 200 SOAP ACK  │ Kafka: kraken-rcdc-events
   ▼                    ▼
  CIS         ┌─────────────────────────┐
              │ RcdcRequestKafkaListner │
              │ Call SOA-RCDC service   │
              │ http://192.168.4.34:    │
              │ 9000/rcdcrequest (JSON) │
              └─────────────────────────┘
                         │
              ┌──────────┴──────────┐
            200 OK              5xx/Error
              │                    │
           Success           Retry / DLQ
              │
   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
   (Later — HES executes switch and calls back)

  HES
   │
   │  POST /api/v1/rcdc/response  (JSON)
   ▼
┌─────────────────────────────────┐
│  RcdcHesResponseHttpListner     │
│  1. Validate JSON body           │
│  2. Publish to Kafka             │
│  3. Return HTTP 202              │
└─────────────────────────────────┘
   │                    │
   │ HTTP 202           │ Kafka: kraken-rcdc-hes-response-events
   ▼                    ▼
  HES         ┌────────────────────────────────┐
              │  RcdcResponseHesKafkaListner    │
              │  1. Deserialise JSON            │
              │  2. Build SOAP envelope         │
              │  3. POST to MDM SOAP service    │
              │     http://192.168.4.34:9000/  │
              │     rcdcswitchstatus            │
              │  4. Inspect for SOAP faults     │
              └────────────────────────────────┘
```

---

### Leg 1A — Inbound Command (HTTP → Kafka)

**Endpoint:** `POST http://host:8122/api/v1/rcdc`  
**Content-Type:** `text/xml`

#### Request Format

```xml
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:cim="http://xmlns.oracle.com/ouaf/iec">
  <soapenv:Header/>
  <soapenv:Body>
    <cim:requestMessage>
      <cim:header>
        <cim:verb>CHANGE</cim:verb>
        <cim:noun>RCDSwitchState</cim:noun>
        <cim:correlationID>rcdc-AW8003092-uuid-001</cim:correlationID>
        <cim:timeStamp>2026-05-08T11:22:21.813Z</cim:timeStamp>
      </cim:header>
      <cim:payLoad>
        <cim:RCDSwitchState>
          <cim:endDeviceAsset>
            <cim:mRID>AW8003092</cim:mRID>
          </cim:endDeviceAsset>
          <cim:state>DISCONNECT</cim:state>
        </cim:RCDSwitchState>
      </cim:payLoad>
    </cim:requestMessage>
  </soapenv:Body>
</soapenv:Envelope>
```

#### Response Format (HTTP 200 — Accepted)

```xml
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
  <soapenv:Header/>
  <soapenv:Body>
    <RcdcAcknowledgement>
      <instanceID>7823649102</instanceID>
      <reply><replyCode>0.0</replyCode></reply>
    </RcdcAcknowledgement>
  </soapenv:Body>
</soapenv:Envelope>
```

#### Scenarios

| # | Scenario | Trigger | Result |
|---|----------|---------|--------|
| 1A-S1 | **Happy Path** | Valid SOAP request | HTTP 200 ACK, event published to `kraken-rcdc-events` |
| 1A-S2 | **Missing correlationID** | `<correlationID/>` empty | HTTP 400, message NOT published to Kafka |
| 1A-S3 | **Missing mRID** | `<mRID/>` empty | HTTP 400, message NOT published to Kafka |
| 1A-S4 | **Invalid state** | state = "TOGGLE" | HTTP 400, must be `CONNECT` or `DISCONNECT` |
| 1A-S5 | **Malformed XML** | Non-XML body | HTTP 422 Transformation Error |
| 1A-S6 | **Kafka unavailable** | Broker down | Retry 3× with backoff → HTTP 503 |

---

### Leg 1B — Kafka Consumer → SOA-RCDC

**Topic:** `kraken-rcdc-events`  
**Target:** `POST http://192.168.4.34:9000/rcdcrequest`  
**Content-Type:** `application/json`

#### Scenarios

| # | Scenario | HTTP Response | Result |
|---|----------|---------------|--------|
| 1B-S1 | **Happy Path** | 200 OK | Offset committed, success |
| 1B-S2 | **Service 500** | 500 / 503 | Retry 3× → `kraken-rcdc-retry-events` |
| 1B-S3 | **Service timeout** | Connection timeout | Retry 3× → `kraken-rcdc-retry-events` |
| 1B-S4 | **Service 400** | 400 Bad Request | DLQ immediately → `kraken-rcdc-dlq-events` |
| 1B-S5 | **Retry exhausted** | Still failing after 9 total attempts | Final DLQ → `kraken-rcdc-dlq-events` |

---

### Leg 1C — HES Callback (HTTP → Kafka)

**Endpoint:** `POST http://host:8122/api/v1/rcdc/response`  
**Content-Type:** `application/json`

#### Request Format

```json
{
  "header": {
    "correlationID": "rcdc-AW8003092-uuid-001",
    "verb": "REPLY",
    "noun": "RCDSwitchState"
  },
  "payload": {
    "defaultResponse": {
      "endDeviceAsset": { "mRID": "AW8003092" }
    }
  },
  "reply": { "replyCode": "0.0" }
}
```

**Response:** HTTP 202 (Accepted — asynchronous processing)

---

### Leg 1D — Kafka Consumer → MDM SOAP

**Topic:** `kraken-rcdc-hes-response-events`  
**Target:** `POST http://192.168.4.34:9000/rcdcswitchstatus`  
**Content-Type:** `text/xml; charset=utf-8`  
**SOAPAction:** `""`

#### SOAP Payload Sent to MDM

```xml
<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:msg="http://www.iec.ch/TC57/2008/schema/message"
                  xmlns:ns2="http://iec.ch/TC57/2009/EndDeviceAssets#"
                  xmlns:ns3="http://www.trilliantinc.com/SEAL/1.0/dt025pvvnl">
  <soapenv:Header/>
  <soapenv:Body>
    <msg:RequestMessage>
      <msg:Header>
        <msg:Verb>CHANGE</msg:Verb>
        <msg:Noun>RCDSwitchState</msg:Noun>
        <msg:Timestamp>2026-05-08T11:22:42.364Z</msg:Timestamp>
        <msg:Source>KRAKEN-CIS</msg:Source>
        <msg:CorrelationID>rcdc-AW8003092-uuid-001</msg:CorrelationID>
      </msg:Header>
      <msg:Reply>
        <msg:ReplyCode>0.0</msg:ReplyCode>
      </msg:Reply>
      <msg:Payload>
        <ns3:RCDSwitchState>
          <ns3:EndDeviceAsset>
            <ns2:mRID>AW8003092</ns2:mRID>
          </ns3:EndDeviceAsset>
        </ns3:RCDSwitchState>
      </msg:Payload>
    </msg:RequestMessage>
  </soapenv:Body>
</soapenv:Envelope>
```

#### Scenarios

| # | Scenario | MDM Response | Result |
|---|----------|-------------|--------|
| 1D-S1 | **Happy Path** | HTTP 200, no SOAP fault | Offset committed, success |
| 1D-S2 | **SOAP Server Fault** | HTTP 200 + `<soap:Fault><faultcode>Server</faultcode>` | Treated as HTTP 500, retry → `kraken-rcdc-hes-retry-events` |
| 1D-S3 | **SOAP Client Fault** | HTTP 200 + `<soap:Fault><faultcode>Client</faultcode>` | Treated as HTTP 400, DLQ immediately |
| 1D-S4 | **HTTP 503** | 503 Service Unavailable | Retry 3× → `kraken-rcdc-hes-retry-events` |
| 1D-S5 | **Network Error** | TCP refused / timeout | Retry 3× → `kraken-rcdc-hes-retry-events` |
| 1D-S6 | **Retry exhausted** | Still failing after 9 total attempts | Final DLQ → `kraken-rcdc-hes-dlq-events` |

---

## 4. Use Case 2 — OnDemandRead (ODR)

### Business Purpose

CIS requests a real-time meter reading for a specific device. Kraken validates the request, forwards it to a mock SOAP service, and returns the meter readings synchronously in the same HTTP response. **No Kafka is involved — this is fully synchronous.**

---

### Complete Flow Diagram

```
  CIS
   │
   │  POST /api/v1/odr  (SOAP XML)
   ▼
┌───────────────────────────────────────┐
│  OdrHttpListner                        │
│  1. Save full SOAP envelope (rawXml)  │
│  2. Strip SOAP envelope               │
│  3. JAXB unmarshal → OdrRequest       │
│  4. Validate correlationID + mRID     │
│  5. POST rawXml to ODR mock service   │
│     http://192.168.4.34:9000/         │
│     ondemandread  (same SOAP payload) │
│  6. Receive CM-GetMeterReadingsResp   │
│  7. Wrap response in SOAP envelope    │
│  8. Return HTTP 200 SOAP response     │
└───────────────────────────────────────┘
   │
   │  HTTP 200 — SOAP Envelope (CM-GetMeterReadingsResp with 44 readings)
   ▼
  CIS
```

---

**Endpoint:** `POST http://host:8122/api/v1/odr`  
**Content-Type:** `text/xml`

#### Request Format

```xml
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:cim="http://xmlns.oracle.com/ouaf/iec">
  <soapenv:Header/>
  <soapenv:Body>
    <cim:requestMessage>
      <cim:header>
        <cim:verb>GET</cim:verb>
        <cim:noun>MeterReadings</cim:noun>
        <cim:replyAddress>Default</cim:replyAddress>
        <cim:correlationID>odr_req_AW8003092_5c18c4e2-7e81-402f-97cc</cim:correlationID>
        <cim:timeStamp>2026-05-08T11:22:21.813Z</cim:timeStamp>
      </cim:header>
      <cim:payLoad>
        <cim:meterReadings>
          <cim:endDeviceAsset>
            <cim:mRID>AW8003092</cim:mRID>
          </cim:endDeviceAsset>
          <cim:profile>false</cim:profile>
          <cim:currentRead>true</cim:currentRead>
          <cim:daily>false</cim:daily>
          <cim:monthly>false</cim:monthly>
          <cim:instantaneous>false</cim:instantaneous>
          <cim:event>false</cim:event>
          <cim:reading>false</cim:reading>
          <cim:dateRange>
            <cim:startTime>2026-05-08T11:22:21.813Z</cim:startTime>
            <cim:endTime>2026-05-08T11:22:21.813Z</cim:endTime>
          </cim:dateRange>
        </cim:meterReadings>
      </cim:payLoad>
    </cim:requestMessage>
  </soapenv:Body>
</soapenv:Envelope>
```

#### Response Format (HTTP 200)

```xml
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
  <soapenv:Header/>
  <soapenv:Body>
    <CM-GetMeterReadingsResp xmlns="http://ouaf.oracle.com/webservices/cm/CM-GetMeterReadingsResp"
                             xmlns:tns="http://ouaf.oracle.com/webservices/cm/CM-GetMeterReadingsResp">
      <tns:responseDetail>
        <tns:header>
          <tns:verb>REPLY</tns:verb>
          <tns:noun>MeterReadings</tns:noun>
          <tns:correlationID>odr_req_AW8003092_5c18c4e2-7e81-402f-97cc</tns:correlationID>
          <tns:timeStamp>2026-05-08T11:22:42.364Z</tns:timeStamp>
        </tns:header>
        <tns:payload>
          <tns:meterReadings>
            <tns:meterReading>
              <tns:readings>
                <tns:sequence>1</tns:sequence>
                <tns:timestamp>2026-05-08T16:49:00+05:30</tns:timestamp>
                <tns:value>500.0</tns:value>
                <tns:readingType>13.0.0.9.1.1.12.0.0.0.0.1.0.0.0.3.71.0</tns:readingType>
              </tns:readings>
              <!-- ... 44 readings total ... -->
              <tns:meterAsset>
                <tns:mRID>AW8003092</tns:mRID>
              </tns:meterAsset>
            </tns:meterReading>
          </tns:meterReadings>
        </tns:payload>
        <tns:reply>
          <tns:replyCode>0.0</tns:replyCode>
        </tns:reply>
      </tns:responseDetail>
    </CM-GetMeterReadingsResp>
  </soapenv:Body>
</soapenv:Envelope>
```

#### Scenarios

| # | Scenario | Trigger | Result |
|---|----------|---------|--------|
| 2-S1 | **Happy Path** | Valid SOAP request | HTTP 200 with meter readings |
| 2-S2 | **Missing correlationID** | Empty correlationID | HTTP 400, mock never called |
| 2-S3 | **Missing mRID** | Empty mRID | HTTP 400, mock never called |
| 2-S4 | **Mock service down** | Connection refused | Retry 3× → HTTP 503 with retry count |
| 2-S5 | **Mock returns SOAP fault** | SOAP Fault in HTTP 200 | Retry (Server fault) or HTTP 400 (Client fault) |
| 2-S6 | **Malformed XML request** | Non-XML or wrong namespace | HTTP 422 Transformation Error |

---

## 5. Use Case 3 — Alarm Events

### Business Purpose

HES sends meter alarm events (tamper alerts, voltage anomalies, communication faults, etc.) to Kraken. Kraken validates and publishes them to Kafka for downstream CIS processing.

---

**Endpoint:** `POST http://host:8122/api/v1/alarms`  
**Content-Type:** `application/json`

#### Request Format

```json
{
  "header": {
    "correlationID": "alarm-AW8003092-20260508-001",
    "verb": "CHANGE",
    "noun": "EndDeviceEvents"
  },
  "payload": {
    "alarmEvent": {
      "endDeviceAsset": { "mRID": "AW8003092" },
      "eventType": "TAMPER_DETECTED",
      "severity": "HIGH",
      "timestamp": "2026-05-08T11:22:21.813Z"
    }
  }
}
```

**Response:** HTTP 202 (Accepted)

#### Kafka Topic

`kraken-alarm-events`

#### Scenarios

| # | Scenario | Trigger | Result |
|---|----------|---------|--------|
| 3-S1 | **Happy Path** | Valid JSON alarm | HTTP 202, published to `kraken-alarm-events` |
| 3-S2 | **Missing correlationID** | Empty header | HTTP 400 |
| 3-S3 | **Invalid JSON** | Malformed body | HTTP 422 |
| 3-S4 | **Kafka unavailable** | Broker down | Retry 3× → HTTP 503 |

---

## 6. Use Case 4 — Profile Reads (FTP / S3)

### Business Purpose

Large CSV files containing meter profile readings (interval data, daily/monthly energy values) are polled from FTP or S3 on a scheduled basis. Each file is parsed, batched, and published to Kafka for downstream metering systems.

---

### Complete Flow Diagram

```
FTP: ftp://192.168.4.34:21/Reads/profilereads/*.csv
  OR
S3 Bucket (configured via s3.yml)
            │
            │  Poll every 60 seconds
            ▼
┌──────────────────────────────────┐
│  ProfileReadsFTPListner          │
│  OR ProfileReadsS3Listner        │
│                                  │
│  ProfileReadsCsvProcessor:       │
│  1. Parse CSV rows               │
│  2. Batch 500 rows at a time     │
│  3. Publish each batch to Kafka  │
└──────────────────────────────────┘
            │
     ┌──────┼──────────────────────┐
     │      │                      │
  Success  Failed rows          Audit
     │      │                      │
     ▼      ▼                      ▼
  kraken-  kraken-profile-     kraken-profile-
  profile- reads-dlq-events    reads-audit-events
  reads-
  events

  File moved to:
  ✓ Success → /Reads/Archive
  ✗ Error   → /Reads/Error
```

#### Scenarios

| # | Scenario | Trigger | Result |
|---|----------|---------|--------|
| 4-S1 | **Happy Path** | Valid CSV file found | All rows batched (500/batch) → `kraken-profile-reads-events`, file → /Reads/Archive |
| 4-S2 | **Partially corrupt CSV** | Some rows malformed | Valid rows → `kraken-profile-reads-events`, failed rows → `kraken-profile-reads-dlq-events`, audit → `kraken-profile-reads-audit-events` |
| 4-S3 | **File > 100 MB** | Large file | Stream-cached to disk (prevents OOM), processed normally |
| 4-S4 | **FTP read error** | Network/connection fail | File moved to /Reads/Error |
| 4-S5 | **No files** | Empty directory | Poller waits for next 60s cycle |
| 4-S6 | **Duplicate file** | Same file re-appears | Read-lock (`changed`) prevents duplicate processing |

---

## 7. SOAP Envelope Handling

All SOAP-based endpoints in Kraken CIS handle SOAP envelopes in both directions transparently.

### Inbound — Stripping SOAP Envelope

**Component:** `SoapEnvelopeExtractorProcessor`

| Behaviour | Detail |
|-----------|--------|
| Detects SOAP 1.1 | Namespace: `http://schemas.xmlsoap.org/soap/envelope/` |
| Detects SOAP 1.2 | Namespace: `http://www.w3.org/2003/05/soap-envelope` |
| Extracts body | First child element of `<soapenv:Body>` → set as exchange body for JAXB |
| Plain XML pass-through | If body is not a SOAP envelope → unchanged (backward compatible) |
| Security | XXE prevention: DOCTYPE and external entities disabled |

### Outbound Response — Wrapping in SOAP Envelope

**Component:** `SoapEnvelopeWrapperProcessor`

Applied to responses for RCDC and ODR endpoints:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
  <soapenv:Header/>
  <soapenv:Body>
    <!-- inner XML response placed here -->
  </soapenv:Body>
</soapenv:Envelope>
```

### Outbound Call — MDM SOAP Notification

**Component:** `RcdcMdmNotificationMapper`  

Builds a complete SOAP 1.1 document with proper namespaces (`msg`, `ns2`, `ns3`) before sending to MDM. Includes SOAP fault detection on the response — even HTTP 200 responses are inspected for `<soap:Fault>` and rerouted appropriately.

### Summary Table

| Direction | Endpoint | SOAP Handling |
|-----------|----------|--------------|
| Inbound request | `/api/v1/rcdc` | Strip envelope → JAXB unmarshal |
| Response to caller | `/api/v1/rcdc` | Wrap acknowledgement in SOAP envelope |
| Inbound request | `/api/v1/odr` | Strip envelope, save raw for forwarding |
| Response to caller | `/api/v1/odr` | Wrap mock response in SOAP envelope |
| Outbound to MDM | `rcdcswitchstatus` | Build full SOAP 1.1 envelope |
| Outbound to ODR mock | `ondemandread` | Forward original SOAP envelope as-is |

---

## 8. Retry & Error Policy

### Two-Layer Retry Architecture

```
LAYER 1 — In-Process Camel Retry (immediate, within same Kafka consumer)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HTTP call fails (5xx / 404 / 408 / 429 / network error)
   │
   ├─ Attempt 1  → wait 1s
   ├─ Attempt 2  → wait 2s
   └─ Attempt 3  → wait 4s
        │
        └─ All fail → X-Error-Attempt = 3
                    → publish to RETRY TOPIC

LAYER 2 — Retry Queue Consumer (slow poll, separate consumer group)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Retry topic polled every 5 minutes  ← 5 min wait IS the back-off
   │
   ├─ X-Error-Attempt < 3 → retry (another 3 in-process attempts)
   │      ├─ Success → done ✓
   │      └─ Fail → X-Error-Attempt += 3 → re-publish to retry topic
   │
   └─ X-Error-Attempt >= 3 → EXHAUSTED → publish to DLQ
```

### Total Attempts Before DLQ

```
Pass 1 (main consumer):   3 attempts → X-Error-Attempt: 3  → retry topic
Pass 2 (retry, +5 min):   3 attempts → X-Error-Attempt: 6  → retry topic
Pass 3 (retry, +5 min):   X-Error-Attempt = 6 >= 3 → DLQ

Total = 9 call attempts over ~10 minutes
```

### Error Routing Decision Tree

```
Exception thrown
   │
   ├─ ValidationException / TransformationException
   │       → DLQ immediately (payload is permanently invalid — no retry)
   │
   ├─ ExternalServiceException (4xx response)
   │       → DLQ immediately (downstream permanently rejected the request)
   │
   ├─ SOAP Client Fault (detected in HTTP 200 body)
   │       → Mapped to HTTP 400 → DLQ immediately
   │
   ├─ RetryQueueExhaustedException (X-Error-Attempt >= 3)
   │       → DLQ immediately (all retry chances used)
   │
   ├─ RetryableException (5xx / 404 / 408 / 429 / network / SOAP Server Fault)
   │       → 3× in-process retry (1s → 2s → 4s) → retry topic
   │
   └─ Unknown Exception
           In main consumer  → 3× retry → retry topic
           In retry consumer → DLQ immediately (treat as permanent on second chance)
```

### Error Scenarios Quick Reference

| Error | Category | Retried? | Topic |
|-------|----------|----------|-------|
| Missing required field | Validation | No | DLQ |
| Malformed XML / JSON | Transformation | No | DLQ |
| Downstream HTTP 400 | Client Error | No | DLQ |
| Downstream HTTP 401 / 403 | Client Error | No | DLQ |
| SOAP Client Fault | Client Error | No | DLQ |
| Downstream HTTP 500 | Server Error | Yes (9 attempts) | Retry → DLQ |
| Downstream HTTP 503 | Server Error | Yes (9 attempts) | Retry → DLQ |
| Downstream HTTP 429 | Rate Limit | Yes (9 attempts) | Retry → DLQ |
| Connection timeout | Network | Yes (9 attempts) | Retry → DLQ |
| TCP refused | Network | Yes (9 attempts) | Retry → DLQ |
| SOAP Server Fault | Server Error | Yes (9 attempts) | Retry → DLQ |

---

## 9. Kafka Topics Reference

| Topic | Purpose | Producer | Consumer | Retention |
|-------|---------|----------|---------|-----------|
| `kraken-alarm-events` | Meter alarm events | AlarmHttpListner | Downstream systems | — |
| `kraken-rcdc-events` | RCDC connect/disconnect commands | RcdcRequestHttpListner | RcdcRequestKafkaListner | — |
| `kraken-rcdc-retry-events` | RCDC retry queue (transient failures) | RcdcRequestKafkaListner | RcdcRetryKafkaRoute | — |
| `kraken-rcdc-dlq-events` | RCDC dead-letter queue | System | Ops / Alerting | Extended |
| `kraken-rcdc-hes-response-events` | HES callback events | RcdcHesResponseHttpListner | RcdcResponseHesKafkaListner | — |
| `kraken-rcdc-hes-retry-events` | MDM SOAP retry queue | RcdcResponseHesKafkaListner | RcdcHesRetryKafkaRoute | — |
| `kraken-rcdc-hes-dlq-events` | MDM SOAP dead-letter queue | System | Ops / Alerting | Extended |
| `kraken-profile-reads-events` | Meter profile readings (batched) | ProfileReadsCsvProcessor | Downstream systems | — |
| `kraken-profile-reads-dlq-events` | Failed CSV rows | ProfileReadsCsvProcessor | Ops / Alerting | Extended |
| `kraken-profile-reads-audit-events` | Processing audit trail | ProfileReadsAuditRoute | Ops / Reporting | Extended |

### Kafka Producer Configuration

| Setting | Value | Purpose |
|---------|-------|---------|
| `acks` | `all` | All in-sync replicas must acknowledge |
| `retries` | `3` | Producer-level retries |
| `compression-type` | `lz4` | Compress messages for network efficiency |
| `max-in-flight-request` | `5` | Cap for idempotent delivery |
| `linger-ms` | `5` | Small batching delay for throughput |

### Kafka Consumer Configuration

| Setting | Value | Purpose |
|---------|-------|---------|
| `enable-auto-commit` | `false` | Manual commit only after successful processing |
| `auto-offset-reset` | `earliest` | Process from beginning on new consumer group |
| `max-poll-records` | `500` | Max records per poll (main consumers) |
| `max-poll-records` | `50` | Max records per poll (retry consumers — slower) |
| `consumers-count` | `2` | Parallel consumer threads per route |
| `session-timeout-ms` | `30000` | 30 seconds |
| `max-poll-interval-ms` | `300000` | 5 minutes — matches retry poll delay |

---

## 10. External Service Endpoints

| Service Key | URL | Method | Content-Type | Timeout | Notes |
|-------------|-----|--------|-------------|---------|-------|
| `rcdc` | `http://192.168.4.34:9000/rcdcrequest` | POST | `application/json` | 30s | SOA-RCDC Target |
| `rcdc-mdm-notification` | `http://192.168.4.34:9000/rcdcswitchstatus` | POST | `text/xml; charset=utf-8` | 30s | MDM SOAP (SOAPAction: `""`) |
| `odr-mock` | `http://192.168.4.34:9000/ondemandread` | POST | `text/xml; charset=utf-8` | 30s | ODR Mock SOAP (SOAPAction: `""`) |

### HTTP Client Retry Configuration

| Setting | Value |
|---------|-------|
| `max-attempts` | `3` |
| `initial-delay-ms` | `1000` (1 second) |
| `backoff-multiplier` | `2.0` |
| `max-delay-ms` | `30000` (30 seconds) |
| `retryable-status-codes` | `404, 408, 429, 500, 502, 503, 504` |

---

## 11. Observability & Monitoring

### Health & Management Endpoints (Port 8121)

| Endpoint | Purpose | Example Response |
|----------|---------|-----------------|
| `GET /actuator/health` | Application + Kafka health check | `{"status": "UP", "components": {"kafka": {"status": "UP"}}}` |
| `GET /actuator/prometheus` | Prometheus metrics scrape endpoint | Metrics in Prometheus text format |
| `GET /actuator/camelroutes` | All active Camel route IDs and status | Route list with state (Started/Stopped) |
| `GET /actuator/camelstats` | Camel context statistics | Exchange counts, processing times |
| `GET /actuator/metrics` | All Micrometer metrics | Filtered metric data |
| `GET /actuator/info` | Application info | Name, version |

### API Documentation (Port 8122)

| Endpoint | Purpose |
|----------|---------|
| `GET /api-doc` | OpenAPI / Swagger JSON specification |

### Active Camel Route IDs

| Route ID | Description |
|----------|-------------|
| `route-post-rcdc` | RCDC HTTP ingest |
| `route-post-odr` | ODR HTTP ingest |
| `route-rcdc-kafka-consumer` | RCDC Kafka consumer → SOA-RCDC |
| `route-rcdc-hes-kafka-consumer` | HES response Kafka consumer → MDM |
| `route-rcdc-retry-consumer` | RCDC retry queue consumer |
| `route-rcdc-hes-retry-consumer` | HES retry queue consumer |

### Log Files

| File | Content |
|------|---------|
| `logs/kraken-cis.log` | Plain text rolling log (dev/test) |
| `logs/kraken-cis-json.log` | JSON structured log (uat/prod — for ELK/Splunk) |
| `logs/kraken-cis-audit.log` | Audit trail — route entry/exit with duration |

---

## 12. Distributed Tracing & Logging

Every log line produced within a Camel route is automatically stamped with four context fields:

| Field | MDC Key | Source | Example |
|-------|---------|--------|---------|
| Correlation ID | `correlationId` | Inbound `X-Correlation-ID` header, or UUID-generated | `odr_req_AW8003092_5c18c4e2` |
| Trace ID | `traceId` | OTel span (Micrometer Tracing bridge) | `3f2a1b4c8d9e0f12` |
| Span ID | `spanId` | OTel span (Micrometer Tracing bridge) | `7c3d1e4a` |
| Route ID | `routeId` | Camel route | `route-post-odr` |

### Sample Log Output (Console — dev profile)

```
10:23:45.123 INFO  OdrHttpListner        [cid=odr_req_AW8003092] [tid=3f2a1b4c8d9e0f12 sid=7c3d1e4a] [route-post-odr] - routeEntry
10:23:45.234 INFO  OdrRequestProcessor   [cid=odr_req_AW8003092] [tid=3f2a1b4c8d9e0f12 sid=7c3d1e4a] [route-post-odr] - odrRequestReceived mRID=AW8003092
10:23:45.456 INFO  SOAOdrMockService     [cid=odr_req_AW8003092] [tid=3f2a1b4c8d9e0f12 sid=7c3d1e4a] [route-post-odr] - odrMockServiceSendRequest
10:23:45.789 INFO  OdrMockCallProcessor  [cid=odr_req_AW8003092] [tid=3f2a1b4c8d9e0f12 sid=7c3d1e4a] [route-post-odr] - odrMockCallCompleted mRID=AW8003092
10:23:45.801 INFO  RouteLoggingProcessor [cid=odr_req_AW8003092] [tid=3f2a1b4c8d9e0f12 sid=7c3d1e4a] [route-post-odr] - routeExit durationMs=678
```

### OTel Span Lifecycle Per Route

```
Route Entry (populateMDC)
   │
   ├─ tracer.nextSpan("camel.route-post-odr").start()
   └─ tracer.withSpan(span)  → MDC gets traceId + spanId
         │
         │  All log lines carry traceId + spanId
         │
Route Exit (exit processor)
   ├─ auditLogger.logRouteEnd()  ← last log with traceId/spanId
   ├─ scope.close()              ← OTel bridge removes traceId/spanId from MDC
   ├─ span.end()                 ← span exported to Zipkin/Jaeger/OTLP
   └─ clearMDC()                 ← correlationId, routeId etc. removed
```

### Correlation ID Propagation

```
Inbound HTTP  →  X-Correlation-ID header (if present) is reused
              →  If absent, UUID is generated
              →  Stamped on all log lines within the route
              →  Returned in X-Correlation-ID response header
              →  Used as Kafka message key for traceability
              →  Included in all outbound SOAP/JSON payloads
```

---

## Appendix — Error Response Formats

### HTTP 400 — Validation Error

```json
{
  "errorCode": "MISSING_FIELD",
  "errorCategory": "VALIDATION",
  "field": "endDeviceAsset.mRID",
  "correlationId": "rcdc-AW8003092-uuid-001",
  "timestamp": "2026-05-08T11:22:21.813Z"
}
```

### HTTP 422 — Transformation Error

```json
{
  "errorCode": "MAPPING_FAILED",
  "errorCategory": "TRANSFORMATION",
  "message": "Failed to unmarshal XML payload",
  "correlationId": "rcdc-AW8003092-uuid-001"
}
```

### HTTP 503 — System / Retry Exhausted

```json
{
  "errorCode": "SYSTEM_ERROR",
  "errorCategory": "SYSTEM",
  "message": "Service temporarily unavailable after 3 retries",
  "retryCount": 3,
  "correlationId": "rcdc-AW8003092-uuid-001"
}
```

---

*Document prepared for PGE Kraken CIS Client Demo — June 2026*  
*Application version: 1.0.0-SNAPSHOT | Camel 4.6.0 | Spring Boot 3.2.5*
