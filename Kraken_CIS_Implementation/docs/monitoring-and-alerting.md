# Monitoring and Alerting Guide

**Platform:** Kraken CIS Integration — Apache Camel 4.6 / Spring Boot 3.2

---

## 1. Observability Stack

| Layer | Built-in | Requires external tooling |
|-------|---------|--------------------------|
| Health checks | ✅ `/actuator/health` (includes Kafka) | — |
| Camel route metrics | ✅ `/actuator/camelroutes`, `/actuator/camelstats` | — |
| Prometheus scrape | ✅ `/actuator/prometheus` | Prometheus server |
| DLQ depth | ✅ `/api/v1/admin/dlq/stats` | — |
| Structured JSON logs | ✅ `logs/kraken-cis-json.log` (Logstash encoder) | ELK / Splunk |
| Audit log | ✅ `logs/kraken-cis-audit.log` | — |
| Distributed traces | ✅ traceId/spanId in every log line | Zipkin / Jaeger / Tempo |
| Alerting | ❌ | AlertManager / PagerDuty |
| Dashboards | ❌ | Grafana |

---

## 2. Prometheus Metrics

Scrape endpoint: `GET http://localhost:9091/actuator/prometheus`

### Key metrics

| Metric | Labels | Description |
|--------|--------|-------------|
| `camel_exchanges_total` | `routeId`, `application` | Total exchanges processed |
| `camel_exchanges_failed_total` | `routeId` | Failed exchange count |
| `camel_exchange_processing_time_seconds` | `routeId` | Processing time histogram |
| `kafka_consumer_records_lag` | `consumerGroup`, `partition`, `topic` | Consumer group lag |
| `kafka_consumer_fetch_rate` | `consumerGroup` | Records fetched per second |
| `kafka_producer_record_send_rate` | `clientId` | Records sent per second |
| `jvm_memory_used_bytes` | `area` (heap/nonheap) | JVM memory |
| `jvm_gc_pause_seconds` | `action`, `cause` | GC pause duration |
| `http_server_requests_seconds` | `uri`, `status`, `method` | HTTP request latency |

### Example PromQL queries

```promql
# Camel failure rate per route (last 5 min)
rate(camel_exchanges_failed_total{application="kraken-cis"}[5m])

# RCDC consumer lag
kafka_consumer_records_lag{consumerGroup="kraken-cis-group", topic="kraken-rcdc-events"}

# HTTP 5xx rate on alarm endpoint
rate(http_server_requests_seconds_count{uri="/api/v1/alarms",status=~"5.."}[5m])

# 99th percentile processing time for RCDC consumer
histogram_quantile(0.99,
  rate(camel_exchange_processing_time_seconds_bucket{routeId="route-rcdc-kafka-consumer"}[5m])
)

# JVM heap usage percentage
jvm_memory_used_bytes{area="heap"} / jvm_memory_max_bytes{area="heap"} * 100
```

---

## 3. Grafana Dashboard Recommendations

### Dashboard 1 — Integration Overview

| Panel | Metric | Type |
|-------|--------|------|
| Total exchanges/min | `rate(camel_exchanges_total[1m])` | Time series |
| Failed exchanges/min | `rate(camel_exchanges_failed_total[1m])` | Time series (red) |
| Active routes | `camel_routes_running_routes` | Stat |
| DLQ depth (RCDC) | DLQ stats API | Stat |

### Dashboard 2 — Kafka Health

| Panel | Metric | Type |
|-------|--------|------|
| Consumer lag by group | `kafka_consumer_records_lag` | Time series |
| Producer send rate | `kafka_producer_record_send_rate` | Time series |
| Fetch rate | `kafka_consumer_fetch_rate` | Time series |
| KEDA pod count | `kube_deployment_status_replicas` | Stat |

### Dashboard 3 — JVM / Infrastructure

| Panel | Metric | Type |
|-------|--------|------|
| Heap usage % | `jvm_memory_used_bytes{area="heap"}` | Gauge |
| GC pause p99 | `histogram_quantile(0.99, jvm_gc_pause_seconds_bucket)` | Stat |
| Thread count | `jvm_threads_live_threads` | Time series |
| HTTP p99 latency | `histogram_quantile(0.99, http_server_requests_seconds_bucket)` | Time series |

---

## 4. AlertManager Rules

Save as `deployment/alertmanager/rules.yml` and load into Prometheus.

```yaml
groups:
  - name: kraken-cis-alerts
    rules:

      # ── Kafka broker down ────────────────────────────────────────────────
      - alert: KafkaBrokerDown
        expr: up{job="kraken-cis"} == 0 OR kraken_cis_kafka_health == 0
        for: 1m
        labels:
          severity: critical
          team: integration
        annotations:
          summary: "Kafka broker unreachable"
          description: "The Kafka health indicator reports DOWN. All flows are blocked."
          runbook: "docs/operations-playbook.md#3-scenario-a--kafka-broker-down"

      # ── DLQ accumulating ─────────────────────────────────────────────────
      - alert: RcdcDlqDepthWarning
        expr: kraken_cis_dlq_lag{topic="kraken-rcdc-dlq-events"} > 10
        for: 5m
        labels:
          severity: warning
          team: integration
        annotations:
          summary: "RCDC DLQ depth > 10"
          description: "{{ $value }} messages in kraken-rcdc-dlq-events. Investigate X-Error-Type headers."
          runbook: "docs/operations-playbook.md#4-scenario-b--dlq-accumulating"

      - alert: RcdcDlqDepthCritical
        expr: kraken_cis_dlq_lag{topic="kraken-rcdc-dlq-events"} > 50
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "RCDC DLQ critical — {{ $value }} messages"
          runbook: "docs/operations-playbook.md#4-scenario-b--dlq-accumulating"

      # ── Consumer lag ──────────────────────────────────────────────────────
      - alert: KafkaConsumerLagWarning
        expr: |
          sum by (consumerGroup, topic) (
            kafka_consumer_records_lag{consumerGroup=~"kraken-cis-group.*"}
          ) > 500
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Consumer lag {{ $value }} on {{ $labels.consumerGroup }}/{{ $labels.topic }}"
          runbook: "docs/operations-playbook.md#6-scenario-d--high-message-lag--throughput-bottleneck"

      - alert: KafkaConsumerLagCritical
        expr: |
          sum by (consumerGroup, topic) (
            kafka_consumer_records_lag{consumerGroup=~"kraken-cis-group.*"}
          ) > 2000
        for: 3m
        labels:
          severity: critical
        annotations:
          summary: "CRITICAL consumer lag {{ $value }}"

      # ── Camel exchange failure rate ───────────────────────────────────────
      - alert: CamelHighFailureRate
        expr: |
          rate(camel_exchanges_failed_total{routeId=~"route-rcdc.*|route-post.*"}[5m])
          /
          rate(camel_exchanges_total{routeId=~"route-rcdc.*|route-post.*"}[5m])
          > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Camel failure rate > 5% on {{ $labels.routeId }}"

      # ── High processing latency ───────────────────────────────────────────
      - alert: SlowDownstreamService
        expr: |
          histogram_quantile(0.99,
            rate(camel_exchange_processing_time_seconds_bucket{
              routeId="route-rcdc-kafka-consumer"
            }[5m])
          ) > 25
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "SOA-RCDC p99 latency > 25s"
          description: "Downstream service is slow. Check SOA-RCDC availability."

      # ── JVM heap ──────────────────────────────────────────────────────────
      - alert: HighHeapUsage
        expr: |
          jvm_memory_used_bytes{area="heap", application="kraken-cis"}
          / jvm_memory_max_bytes{area="heap", application="kraken-cis"}
          > 0.85
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "JVM heap > 85% on {{ $labels.instance }}"
```

---

## 5. Log-Based Alerting

With ELK / Splunk ingesting `logs/kraken-cis-json.log`:

### Kibana / Splunk alert queries

```
# DLQ routing events — alert when > 5 in 5 minutes
event:"messageRoutedToDlq" | stats count by bin(_time, 5m) | where count > 5

# Retry queue exhausted — page immediately
event:"rcdcRetryQueueExhausted"

# Kafka publish failures
event:"kafkaPublishFailed"

# SOAP client faults (bad mapping — urgent)
event:"soapFaultDetectedInHttpOkResponse" AND isClientFault:true

# Correlation ID trace — find all logs for one request
correlationId:"<CID>"
```

### Example Kibana Alert

```json
{
  "name": "DLQ accumulating",
  "schedule": { "interval": "5m" },
  "conditions": {
    "type": "count",
    "threshold": 5,
    "query": { "match": { "event": "messageRoutedToDlq" } }
  },
  "actions": [
    { "type": "webhook", "url": "https://hooks.slack.com/..." }
  ]
}
```

---

## 6. Health Endpoint Monitoring

```bash
# Kubernetes readiness probe (built into deployment.yaml)
GET http://localhost:9091/actuator/health
Expected: { "status": "UP" }

# Manual check
curl -f http://localhost:9091/actuator/health | jq '.status'

# Kafka component specifically
curl -s http://localhost:9091/actuator/health | jq '.components.kafka'

# All Camel routes running
curl -s http://localhost:9091/actuator/camelroutes \
  | jq '[.[] | select(.status != "Started")] | length'
# Expected: 0 (all routes started)
```

---

## 7. KEDA Scaling Events

```bash
# Watch KEDA ScaledObject status
kubectl describe scaledobject kraken-cis-rcdc-scaler

# Pod count over time
kubectl get hpa -l scaledobject.keda.sh/name=kraken-cis-rcdc-scaler

# KEDA events
kubectl get events --field-selector reason=SuccessfulRescale \
  --sort-by='.lastTimestamp' | tail -20
```

---

## 8. Recommended Alert Channels

| Severity | Channel | Response Time |
|----------|---------|--------------|
| `critical` | PagerDuty → on-call engineer | Immediate |
| `warning` | Slack `#kraken-alerts` | 30 min |
| `info` | Slack `#kraken-info` | Next business day |

### Escalation path

```
Warning (5 min) → Slack #kraken-alerts
Critical (2 min) → PagerDuty → on-call engineer (15 min SLA)
Critical (30 min unacknowledged) → Manager escalation
```
