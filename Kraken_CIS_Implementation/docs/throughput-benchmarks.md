# Throughput Benchmarks & Scaling Guide

**Platform:** Kraken CIS Integration — Apache Camel 4.6 / Spring Boot 3.2

---

## 1. Baseline Configuration

| Knob | Value | Location |
|------|-------|----------|
| Kafka producer `acks` | `all` | `kafka.yml` |
| Kafka producer `linger-ms` | `5` | `kafka.yml` |
| Kafka producer `compression` | `lz4` | `kafka.yml` |
| Kafka producer `maxInFlightRequest` | `5` | `kafka.yml` |
| Kafka consumer `max-poll-records` | `500` | `kafka.yml` |
| Kafka consumer `concurrent-consumers` | `2` | `kafka.yml` |
| Kafka consumer `session-timeout-ms` | `30000` | `kafka.yml` |
| Kafka consumer `max-poll-interval-ms` | `300000` | `kafka.yml` |
| File `maxMessagesPerPoll` | `50` | `ftp.yml` / `s3.yml` |
| File `kafkaBatchSize` | `500` rows | `ftp.yml` / `s3.yml` |
| File `threadPoolSize` | `4` | `ftp.yml` / `s3.yml` |
| KEDA `lagThreshold` | `100` messages | `keda-scaledobject.yaml` |
| KEDA `minReplicas` | `2` | `keda-scaledobject.yaml` |
| KEDA `maxReplicas` | `10` | `keda-scaledobject.yaml` |
| HTTP retry max | `3` attempts | `BaseRoute.java` |
| HTTP retry backoff | `1s → 2s → 4s` | `BaseRoute.java` |

---

## 2. Estimated Throughput (Single Pod, Baseline)

### HTTP Inbound Endpoints

| Endpoint | Payload | Estimated RPS | Bottleneck |
|----------|---------|--------------|------------|
| `POST /api/v1/alarms` | JSON batch (10 events) | 100–200 RPS | Kafka publish latency |
| `POST /api/v1/rcdc` | XML (~500 bytes) | 150–300 RPS | JAXB unmarshal + Kafka |
| `POST /api/v1/rcdc/response` | JSON (~300 bytes) | 200–400 RPS | Kafka publish |

Kong rate limits cap these at 500/min, 200/min, 200/min respectively — throughput is policy-bounded in production.

### Kafka Consumer Routes

| Route | Topic | Estimated TPS | Bottleneck |
|-------|-------|--------------|------------|
| `route-rcdc-kafka-consumer` | `kraken-rcdc-events` | 50–100 msg/s | SOA-RCDC HTTP latency |
| `route-rcdc-hes-kafka-consumer` | `kraken-rcdc-hes-response-events` | 20–50 msg/s | MDM SOAP latency |
| Retry consumers | retry topics | 10–20 msg/s | 5-min poll interval |

> **Key constraint:** `concurrentConsumers=2` means each pod processes 2 messages in parallel per route. With `max-poll-records=500`, each thread fetches up to 500 records per poll cycle but processes them sequentially per partition.

### File Ingestion (FTP / S3)

| Scenario | Rows/file | Files/poll | Estimated time |
|----------|-----------|-----------|----------------|
| Small file | 100 rows | 50 | < 5s/file |
| Medium file | 5,000 rows | 10 | ~30s/file |
| Large file (max) | 100,000 rows | 1 | ~3–5 min/file |

Batch publishing every 500 rows keeps memory < 50 MB regardless of file size.

---

## 3. Vertical Scaling — Per-JVM Knobs

### `concurrent-consumers`

```yaml
kafka:
  consumer:
    concurrent-consumers: 4   # double from baseline
```

| Value | TPS (RCDC consumer) | Memory impact | Risk |
|-------|---------------------|---------------|------|
| 1 | ~25 msg/s | Low | None |
| 2 (baseline) | ~50 msg/s | Medium | None |
| 4 | ~100 msg/s | Medium-High | Must have ≥4 partitions |
| 8 | ~200 msg/s | High | Must have ≥8 partitions; SOA-RCDC must handle concurrency |

**Rule:** `concurrentConsumers` must not exceed partition count on the topic.

### `max-poll-records`

| Value | Batch size | `max-poll-interval-ms` risk | Suitable for |
|-------|-----------|----------------------------|-------------|
| 100 | Small | None | Slow downstream services |
| 500 (baseline) | Medium | Monitor carefully | Balanced |
| 1000 | Large | Increase to 600s | Fast downstream only |

### `kafkaBatchSize` (file routes)

| Value | Max heap for batch | Files/min |
|-------|-------------------|-----------|
| 100 | ~10 MB | More frequent flushes |
| 500 (baseline) | ~50 MB | Balanced |
| 1000 | ~100 MB | Fewer Kafka round-trips |

---

## 4. Horizontal Scaling — Pods + KEDA

### How KEDA scales

```
Consumer lag on kraken-rcdc-events > 100
  → KEDA adds 1 pod per 100 messages of lag
  → Kafka reassigns partitions (rebalance ~15s)
  → New pod starts consuming its partitions
  → Lag decreases
  → After cooldown (120s), KEDA scales back to minReplicas=2
```

### Throughput vs Pod Count

| Pods | `concurrent-consumers=2` | `concurrent-consumers=4` | Notes |
|------|--------------------------|--------------------------|-------|
| 2 (min) | ~100 msg/s | ~200 msg/s | HA baseline |
| 4 | ~200 msg/s | ~400 msg/s | KEDA auto |
| 8 | ~400 msg/s | ~800 msg/s | KEDA auto |
| 10 (max) | ~500 msg/s | ~1000 msg/s | Requires 10+ partitions |

**To unlock 10-pod scaling:** Kafka topics need ≥10 partitions. Current default is likely 1–3.

```bash
# Increase partitions (irreversible)
kafka-topics.sh --bootstrap-server 192.168.4.34:9092 \
  --alter --topic kraken-rcdc-events --partitions 10
```

---

## 5. Bottleneck Analysis

```
HTTP request
  └─ Camel route (< 1ms)
       └─ Kafka publish (5–20ms)  ← usually the bottleneck for HTTP endpoints
            └─ Kafka consumer picks up
                 └─ SOA-RCDC HTTP call (50–500ms)  ← usually the bottleneck for consumers
                      └─ Kafka commit (< 5ms)
```

| Bottleneck | Symptom | Fix |
|------------|---------|-----|
| Kafka publish latency | HTTP p99 > 100ms | Increase `linger-ms` to batch more; check broker IOPS |
| SOA-RCDC slow | Consumer lag growing; high `meanProcessingTime` | Increase `concurrentConsumers`; scale pods |
| MDM SOAP slow | `route-rcdc-hes-kafka-consumer` lag growing | SOAP calls take 100–2000ms; scale pods |
| Kafka broker CPU | High producer/consumer latency | Add brokers; increase partition replicas |
| JVM GC pauses | Spiky processing times | Tune heap: `-Xmx512m -XX:+UseG1GC` |

---

## 6. Running the Load Simulation

```bash
# Prerequisites: curl, GNU parallel
brew install parallel        # macOS
apt install parallel         # Ubuntu

# Basic run — 50 req/s for 2 minutes
./scripts/load-simulation.sh http://localhost:8080 50 120

# High load run — 200 req/s for 5 minutes (demonstrates KEDA scale-out)
./scripts/load-simulation.sh http://localhost:8080 200 300
```

### Three-phase ramp (built into the script)

```
Phase 1 (0–40s):  16 req/s   — ramp up
Phase 2 (40–80s): 50 req/s   — peak load — watch KEDA scale
Phase 3 (80–120s): 10 req/s  — cool down — watch KEDA scale back
```

---

## 7. What to Observe During Load Test

### Actuator (`:9091`)

```bash
# Watch exchange counts every 5s
watch -n 5 "curl -s http://localhost:9091/actuator/camelroutes \
  | jq '[.[] | {id: .id, total: .statistics.exchangesTotal, failed: .statistics.exchangesFailed, mean: .statistics.meanProcessingTime}]'"

# DLQ depth
watch -n 10 "curl -s http://localhost:8080/api/v1/admin/dlq/stats | jq ."
```

### Prometheus metrics to watch

```bash
# Kafka consumer lag (should stay near 0 at 50 req/s; grows at 200 req/s)
curl -s http://localhost:9091/actuator/prometheus | grep 'kafka_consumer_records_lag'

# Camel throughput
curl -s http://localhost:9091/actuator/prometheus | grep 'camel_exchanges_succeeded_total'

# JVM heap
curl -s http://localhost:9091/actuator/prometheus | grep 'jvm_memory_used_bytes.*heap'
```

### Kafka CLI

```bash
# Consumer lag — should increase during phase 2 then drain in phase 3
watch -n 5 "kafka-consumer-groups.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --describe --group kraken-cis-group \
  | awk '{print \$1, \$5, \$6}'"

# KEDA pod count
watch -n 10 "kubectl get pods -l app=kraken-cis --no-headers | wc -l"
```

---

## 8. Recommended Production Configuration

| Knob | Dev | UAT | Prod |
|------|-----|-----|------|
| `concurrent-consumers` | 1 | 2 | 4 |
| `max-poll-records` | 100 | 500 | 500 |
| `kafkaBatchSize` | 100 | 500 | 500 |
| `maxMessagesPerPoll` | 10 | 50 | 50 |
| KEDA `lagThreshold` | — | 100 | 50 |
| KEDA `maxReplicas` | — | 5 | 10 |
| Kafka partitions | 1 | 3 | 10 |
| Kong rate limit `/alarms` | 500/min | 1000/min | 5000/min |
| Tracing sampling | 1.0 | 0.5 | 0.1 |
| JVM heap (`-Xmx`) | 256m | 512m | 1g |
