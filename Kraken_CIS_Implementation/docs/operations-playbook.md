# Kraken CIS Integration Platform — Recovery Operations Playbook

**Platform:** Apache Camel / Spring Boot  
**Kafka broker:** `192.168.4.34:9092`  
**REST API:** `:8080`  **Actuator / management:** `:9091`

---

## Table of Contents
1. [System Overview](#1-system-overview)
2. [Triage — Detecting a Problem](#2-triage--detecting-a-problem)
3. [Scenario A — Kafka Broker Down](#3-scenario-a--kafka-broker-down)
4. [Scenario B — DLQ Accumulating](#4-scenario-b--dlq-accumulating)
5. [Scenario C — Downstream Service Repeatedly Failing](#5-scenario-c--downstream-service-repeatedly-failing)
6. [Scenario D — High Message Lag / Throughput Bottleneck](#6-scenario-d--high-message-lag--throughput-bottleneck)
7. [Scenario E — Invalid Messages Flooding DLQ](#7-scenario-e--invalid-messages-flooding-dlq)
8. [Replay Procedure](#8-replay-procedure)
9. [Monitoring Queries](#9-monitoring-queries)
10. [Alert Thresholds](#10-alert-thresholds)

---

## 1. System Overview

### Ports

| Component | Address | Purpose |
|-----------|---------|---------|
| Camel REST API (Undertow) | `0.0.0.0:8080` | Inbound HTTP endpoints |
| Spring Boot management | `0.0.0.0:9091` | Actuator, health, Camel stats, Prometheus |
| Kafka broker | `192.168.4.34:9092` | All producer and consumer connections |

### Kafka Topics

| Topic | Role | Consumer Group |
|-------|------|----------------|
| `kraken-alarm-events` | Alarm events (fire-and-forget) | — |
| `kraken-rcdc-events` | RCDC commands (primary) | `kraken-cis-group` |
| `kraken-rcdc-retry-events` | RCDC retry queue | `kraken-cis-group-retry` |
| `kraken-rcdc-dlq-events` | RCDC dead letter queue | — |
| `kraken-rcdc-hes-response-events` | HES callbacks (primary) | `kraken-cis-group-mdm` |
| `kraken-rcdc-hes-retry-events` | HES retry queue | `kraken-cis-group-hes-retry` |
| `kraken-rcdc-hes-dlq-events` | HES dead letter queue | — |
| `kraken-profile-reads-events` | Profile reads batch | — |
| `kraken-profile-reads-dlq-events` | Profile reads failed rows | — |

### Retry Flow Summary

```
Exception thrown
  ├─ ValidationException / TransformationException ──► DLQ immediately (no retry)
  ├─ ExternalServiceException (4xx) ─────────────────► DLQ immediately (no retry)
  └─ RetryableException (5xx / 404 / 408 / 429 / network)
          └─ in-process retry ×3 (1s → 2s → 4s)
                  └─ exhausted ──► retry queue (poll every 5 min)
                          └─ ×3 passes, then ────────────────► final DLQ
```

---

## 2. Triage — Detecting a Problem

Run these checks in order to narrow down the failure.

### 2.1 Application Health

```bash
# Overall health (includes kafka, diskSpace, camel)
curl -s http://localhost:9091/actuator/health | jq .

# Compact summary
curl -s http://localhost:9091/actuator/health \
  | jq '{status: .status, components: (.components | to_entries | map({(.key): .value.status}) | add)}'
```

Any component reporting `DOWN` → start with that component's scenario below.

### 2.2 Camel Route Statistics

```bash
# Routes with failures
curl -s http://localhost:9091/actuator/camelroutes \
  | jq '[.[] | select(.statistics.exchangesFailed > 0) | {id: .id, failed: .statistics.exchangesFailed, total: .statistics.exchangesTotal}]'

# All route statuses
curl -s http://localhost:9091/actuator/camelroutes \
  | jq '[.[] | {id: .id, status: .status, total: .statistics.exchangesTotal, failed: .statistics.exchangesFailed}]'
```

### 2.3 DLQ Depth

```bash
# All DLQ topic depths — new endpoint in this application
curl -s http://localhost:8080/api/v1/admin/dlq/stats | jq .
```

### 2.4 Consumer Lag (Kafka CLI)

```bash
kafka-consumer-groups.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --describe --group kraken-cis-group

kafka-consumer-groups.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --describe --group kraken-cis-group-mdm
```

### 2.5 Prometheus Metrics

```bash
# Camel failed exchanges per route
curl -s http://localhost:9091/actuator/prometheus | grep 'camel_exchanges_failed'

# Kafka consumer lag per partition
curl -s http://localhost:9091/actuator/prometheus | grep 'kafka_consumer_records_lag'

# JVM heap
curl -s http://localhost:9091/actuator/prometheus | grep 'jvm_memory_used_bytes.*heap'
```

### 2.6 Application Logs

```bash
# Tail structured log
tail -f logs/kraken-cis.log

# DLQ routing events only
tail -f logs/kraken-cis.log | grep '"event":"messageRoutedToDlq\|messageRoutedToRetryQueue"'

# All failures
grep '"status":"FAILURE"' logs/kraken-cis.log | tail -30

# Trace a specific correlationId
grep '"correlationId":"<CID>"' logs/kraken-cis.log
```

---

## 3. Scenario A — Kafka Broker Down

### Symptoms
- `/actuator/health` → `kafka: DOWN`
- HTTP endpoints return `503` (Kafka publish retries exhausted)
- `kafkaPublishFailed` in logs

### Diagnosis

```bash
# Connectivity test
nc -zv 192.168.4.34 9092
# Windows:
Test-NetConnection -ComputerName 192.168.4.34 -Port 9092

# Broker topic list
kafka-topics.sh --bootstrap-server 192.168.4.34:9092 --list

# Route failure count
curl -s http://localhost:9091/actuator/camelroutes/route-publish-to-kafka \
  | jq '.statistics.exchangesFailed'
```

### Recovery

```bash
# 1 — Restart broker on Kafka host
systemctl restart kafka

# 2 — Verify topics exist
for topic in kraken-rcdc-events kraken-rcdc-retry-events kraken-rcdc-dlq-events \
             kraken-rcdc-hes-response-events kraken-alarm-events; do
  kafka-topics.sh --bootstrap-server 192.168.4.34:9092 --describe --topic "$topic" | head -2
done

# 3 — Confirm application auto-reconnected (no restart needed)
curl -s http://localhost:9091/actuator/health | jq '.components.kafka.status'
```

After the broker is restored, the Camel Kafka producer auto-reconnects. No application restart required.

---

## 4. Scenario B — DLQ Accumulating

### Symptoms
- `GET /api/v1/admin/dlq/stats` returns non-zero `lag`
- `messageRoutedToDlq` log events repeating
- `route-publish-dlq` has rising `exchangesTotal`

### Inspecting DLQ Messages

```bash
# Read RCDC DLQ — prints headers + body (original message unchanged)
kafka-console-consumer.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --topic kraken-rcdc-dlq-events \
  --from-beginning --max-messages 10 \
  --property print.headers=true \
  --property print.key=true \
  --property print.timestamp=true

# HES DLQ
kafka-console-consumer.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --topic kraken-rcdc-hes-dlq-events \
  --from-beginning --max-messages 10 \
  --property print.headers=true

# Profile reads DLQ (failed rows)
kafka-console-consumer.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --topic kraken-profile-reads-dlq-events \
  --from-beginning --max-messages 20 \
  --property print.headers=true
```

### Classifying DLQ Messages

| `X-Error-Type` header | `X-Error-Http-Status` | Cause | Replay? |
|-----------------------|-----------------------|-------|---------|
| `ValidationException` | — | Invalid payload | No — fix upstream |
| `TransformationException` | — | Mapping failure | No — fix code |
| `ExternalServiceException` | `400`–`4xx` | Downstream rejected request | Depends on fix |
| `ExternalServiceException` | `500`–`5xx` | Downstream transient, retries exhausted | Yes |
| `RetryQueueExhaustedException` | — | Retry limit exceeded | Yes |

→ See [Replay Procedure](#8-replay-procedure)

---

## 5. Scenario C — Downstream Service Failing

Covers both `SOA-RCDC-TargetService` and `SOA-MDM-NotificationService`.

### Symptoms
- `httpOutboundAttemptFailed` / `httpOutboundRetriesExhausted` in logs
- DLQ growing on `kraken-rcdc-dlq-events` or `kraken-rcdc-hes-dlq-events`
- Retry queue growing on `kraken-rcdc-retry-events`

### Diagnosis

```bash
# HTTP status codes returned by SOA-RCDC
grep 'httpOutboundAttemptFailed' logs/kraken-cis.log \
  | grep -o '"httpStatus":[0-9]*' | sort | uniq -c | sort -rn

# SOAP faults from MDM
grep 'soapFaultDetectedInHttpOkResponse' logs/kraken-cis.log | tail -20

# Test SOA-RCDC directly
curl -v --max-time 10 http://localhost:8082/api/rcdc/command \
  -H "Content-Type: application/json" -d '{"ping":true}'

# Test MDM SOAP directly
curl -v --max-time 10 http://localhost:8083/ws/RCDSwitchState \
  -H "Content-Type: text/xml; charset=utf-8" -H 'SOAPAction: ""' \
  -d '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body/></soap:Envelope>'
```

### Recovery

1. Restore the downstream service
2. The retry consumer (polling every 5 min) drains `kraken-rcdc-retry-events` automatically
3. For messages that reached DLQ, follow [Replay Procedure](#8-replay-procedure)

Monitor drain:
```bash
watch -n 60 "kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list 192.168.4.34:9092 \
  --topic kraken-rcdc-retry-events --time -1"
```

---

## 6. Scenario D — High Lag / Throughput Bottleneck

### Diagnosis

```bash
# Consumer lag
kafka-consumer-groups.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --describe --group kraken-cis-group

# Route processing time
curl -s http://localhost:9091/actuator/camelroutes/route-rcdc-kafka-consumer \
  | jq '{meanProcessingTime: .statistics.meanProcessingTime, maxProcessingTime: .statistics.maxProcessingTime}'

# Prometheus lag
curl -s http://localhost:9091/actuator/prometheus | grep 'kafka_consumer_records_lag'

# Partition count (max parallelism)
kafka-topics.sh --bootstrap-server 192.168.4.34:9092 \
  --describe --topic kraken-rcdc-events | grep -c 'Partition:'
```

### Scaling Actions

**Vertical — increase concurrent consumers (no restart if using rolling update):**
```yaml
# kafka.yml
kafka:
  consumer:
    concurrent-consumers: 4   # was 2; must be ≤ partition count
```

**Horizontal — add pods (KEDA handles this automatically):**
```bash
kubectl scale deployment kraken-cis-implementation --replicas=5
# or let KEDA do it automatically based on lag
kubectl get scaledobjects
```

**Increase partitions:**
```bash
kafka-topics.sh --bootstrap-server 192.168.4.34:9092 \
  --alter --topic kraken-rcdc-events --partitions 6
# Then increase concurrent-consumers to match
```

---

## 7. Scenario E — Invalid Messages Flooding DLQ

### Symptoms
- DLQ depth growing rapidly with `X-Error-Type: ValidationException` or `TransformationException`
- No downstream service errors — the failure is before the service call
- Recent upstream schema change

### Diagnosis

```bash
# Error type distribution in DLQ
kafka-console-consumer.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --topic kraken-rcdc-dlq-events \
  --from-beginning --max-messages 50 \
  --property print.headers=true \
| grep 'X-Error-Type' | sort | uniq -c | sort -rn

# Sample the raw payload
kafka-console-consumer.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --topic kraken-rcdc-dlq-events \
  --from-beginning --max-messages 3

# Correlate in logs
grep '"errorCode":"VAL-\|TRF-"' logs/kraken-cis.log | tail -20
```

### Error Code Reference

| Code | Class | Meaning | Action |
|------|-------|---------|--------|
| `VAL-002` | `ValidationException` | Missing required field | Fix upstream sender |
| `VAL-003` | `ValidationException` | Invalid format (bad date, wrong state) | Fix upstream sender |
| `TRF-001` | `TransformationException` | Mapping failed | Fix mapper code + redeploy |
| `TRF-003` | `TransformationException` | JSON/XML deserialisation | Schema mismatch — fix upstream |

Messages with `VAL-*` or `TRF-*` are **not replay-eligible** until the root cause is fixed. Work with the upstream system to re-submit corrected messages.

---

## 8. Replay Procedure

DLQ messages preserve the **original body unchanged** and error context in Kafka headers. Original body is ready for direct replay.

### 8.1 Replay RCDC Commands

```bash
# Step 1 — Verify SOA-RCDC is healthy
curl -s http://localhost:9091/actuator/health | jq '.components.kafka.status'
curl -v --max-time 10 http://localhost:8082/api/rcdc/command -H "Content-Type: application/json" -d '{"ping":true}'

# Step 2 — Inspect headers before replaying
kafka-console-consumer.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --topic kraken-rcdc-dlq-events \
  --from-beginning --max-messages 20 \
  --property print.headers=true | grep 'X-Error-Type'

# Step 3 — Replay (bodies only, no headers, to source topic)
kafka-console-consumer.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --topic kraken-rcdc-dlq-events \
  --from-beginning --max-messages 100 \
  --property print.headers=false \
  --property print.key=true \
| kafka-console-producer.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --topic kraken-rcdc-events \
  --property parse.key=true \
  --property key.separator=$'\t'

# Step 4 — Monitor reprocessing
watch -n 5 "curl -s http://localhost:8080/api/v1/admin/dlq/stats | jq '.topics[0]'"
```

### 8.2 Replay HES Response Events

```bash
kafka-console-consumer.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --topic kraken-rcdc-hes-dlq-events \
  --from-beginning --max-messages 100 \
  --property print.key=true \
| kafka-console-producer.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --topic kraken-rcdc-hes-response-events \
  --property parse.key=true \
  --property key.separator=$'\t'
```

### 8.3 Consumer Group Offset Reset (Targeted Replay)

```bash
# ⚠ Stop the consumer before resetting
# Reset to earliest (reprocess all messages)
kafka-consumer-groups.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --group kraken-cis-group \
  --topic kraken-rcdc-events \
  --reset-offsets --to-earliest --execute

# Reset to a point in time
kafka-consumer-groups.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --group kraken-cis-group \
  --topic kraken-rcdc-events \
  --reset-offsets \
  --to-datetime 2026-06-02T10:00:00.000 \
  --execute
```

---

## 9. Monitoring Queries

### Health & Status

```bash
curl -s http://localhost:9091/actuator/health | jq .
curl -s http://localhost:9091/actuator/camelstats | jq .
curl -s http://localhost:8080/api/v1/admin/dlq/stats | jq .
```

### Camel Routes

```bash
# Failure summary
curl -s http://localhost:9091/actuator/camelroutes \
  | jq '[.[] | select(.statistics.exchangesFailed > 0) | {id: .id, failed: .statistics.exchangesFailed}]'

# Individual routes
curl -s http://localhost:9091/actuator/camelroutes/route-rcdc-kafka-consumer | jq .
curl -s http://localhost:9091/actuator/camelroutes/route-rcdc-retry-consumer | jq .
curl -s http://localhost:9091/actuator/camelroutes/route-publish-dlq | jq .
```

### Prometheus

```bash
curl -s http://localhost:9091/actuator/prometheus | grep '^camel_exchanges'
curl -s http://localhost:9091/actuator/prometheus | grep 'kafka_consumer_records_lag'
curl -s http://localhost:9091/actuator/prometheus | grep 'jvm_memory_used_bytes.*heap'
```

### Kafka CLI

```bash
kafka-consumer-groups.sh --bootstrap-server 192.168.4.34:9092 --describe --group kraken-cis-group
kafka-consumer-groups.sh --bootstrap-server 192.168.4.34:9092 --describe --group kraken-cis-group-mdm
kafka-consumer-groups.sh --bootstrap-server 192.168.4.34:9092 --describe --group kraken-cis-group-retry
```

---

## 10. Alert Thresholds

### DLQ Depth

| Topic | Warning | Critical |
|-------|---------|---------|
| `kraken-rcdc-dlq-events` | 10 | 50 |
| `kraken-rcdc-hes-dlq-events` | 10 | 50 |
| `kraken-profile-reads-dlq-events` | 100 rows | 1000 rows |
| `kraken-http-outbound-dlq` | 5 | 25 |

### Consumer Lag

| Consumer Group | Topic | Warning | Critical |
|----------------|-------|---------|---------|
| `kraken-cis-group` | `kraken-rcdc-events` | 500 | 2000 |
| `kraken-cis-group-mdm` | `kraken-rcdc-hes-response-events` | 500 | 2000 |
| `kraken-cis-group-retry` | `kraken-rcdc-retry-events` | 100 | 500 |

### Camel Route Error Rate

| Route | Warning | Critical |
|-------|---------|---------|
| `route-rcdc-kafka-consumer` | >5% | >20% |
| `route-rcdc-hes-kafka-consumer` | >5% | >20% |
| `route-publish-to-kafka` | >0 failures | >5 in 5 min |

### Processing Latency

| Call | Warning | Critical |
|------|---------|---------|
| SOA-RCDC HTTP call | >10s | >25s |
| MDM SOAP call | >15s | >25s |

### Log-Based Alerts

| Log Event | Severity | Action |
|-----------|---------|--------|
| `messageRoutedToDlq` | Warning | Increment DLQ counter alert |
| `rcdcRetryQueueExhausted` | Error | Immediate page |
| `kafkaPublishFailed` | Error | Kafka broker issue |
| `httpOutboundRetriesExhausted` | Warning | Downstream degraded |
| `soapFaultDetectedInHttpOkResponse` with `isClientFault: true` | Error | Invalid payload reaching MDM |
| `kafkaManualCommitHeaderMissing` | Warning | Risk of message reprocessing |
