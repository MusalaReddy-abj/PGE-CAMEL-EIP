# Versioning and Governance Guide

**Platform:** Kraken CIS Integration — Apache Camel 4.6 / Spring Boot 3.2

---

## 1. API Versioning Strategy

### Current approach — URL path versioning

All REST endpoints are prefixed with `/api/v1/` via `RestApiConfig.java`:

```java
restConfiguration()
    .contextPath("/api/v1")
```

| Path | Version |
|------|---------|
| `POST /api/v1/alarms` | v1 |
| `POST /api/v1/rcdc` | v1 |
| `POST /api/v1/rcdc/response` | v1 |

### When to bump to `/api/v2`

| Change type | Action |
|-------------|--------|
| Add optional field | Stay on v1 — backwards compatible |
| Add required field | **Bump to v2** — breaking change |
| Remove field | **Bump to v2** |
| Change field type | **Bump to v2** |
| Change HTTP status codes | **Bump to v2** |
| Change auth scheme | **Bump to v2** |

### Deprecation policy

1. Announce v2 endpoint at least **3 months** before deprecating v1
2. Add `Deprecation` response header on v1 responses: `Deprecation: Sat, 01 Jan 2027 00:00:00 GMT`
3. Run v1 and v2 simultaneously during migration window
4. Remove v1 only after all callers have migrated (verified via access logs)

### Implementation pattern for v2

```java
// RestApiConfig.java — add v2 context
restConfiguration()
    .component("undertow")
    .contextPath("/api")    // shared prefix

// New route class
rest("/v2/rcdc")
    .post()
    .to("direct:process-rcdc-v2");
```

---

## 2. Maven Artifact Versioning

### Convention

| Stage | Version format | Example |
|-------|---------------|---------|
| Development | `MAJOR.MINOR.PATCH-SNAPSHOT` | `1.0.0-SNAPSHOT` |
| Release candidate | `MAJOR.MINOR.PATCH-RC1` | `1.0.0-RC1` |
| Release | `MAJOR.MINOR.PATCH` | `1.0.0` |
| Hotfix | `MAJOR.MINOR.PATCH+1` | `1.0.1` |

### Git tagging

```bash
# Release
git tag -a v1.0.0 -m "Release 1.0.0 — initial GA"
git push origin v1.0.0

# Hotfix
git tag -a v1.0.1 -m "Hotfix — RCDC state validation fix"
git push origin v1.0.1
```

### pom.xml version bump

```bash
mvn versions:set -DnewVersion=1.1.0-SNAPSHOT
mvn versions:commit
```

---

## 3. Kafka Topic Naming Conventions

### Pattern

```
{platform}-{flow}-{role}[-{qualifier}]
```

| Example | Meaning |
|---------|---------|
| `kraken-rcdc-events` | RCDC commands — primary topic |
| `kraken-rcdc-retry-events` | RCDC transient failures — retry queue |
| `kraken-rcdc-dlq-events` | RCDC permanent failures — dead letter |
| `kraken-profile-reads-audit-events` | Profile reads processing audit |

### Change governance

| Change | Impact | Process |
|--------|--------|---------|
| Add optional field to message | Non-breaking | Document in commit, update OpenAPI/schema |
| Add required field | **Breaking** | New topic version (`kraken-rcdc-v2-events`), migrate consumers |
| Remove field | **Breaking** | New topic version |
| Change field type | **Breaking** | New topic version |
| Rename topic | **Breaking** | Create new topic, dual-publish during migration |

### Schema evolution recommendation

For production, add **Confluent Schema Registry** with JSON Schema or Avro:

```yaml
# kafka.yml addition for schema registry
kafka:
  schema-registry:
    url: http://schema-registry:8081
```

Benefits:
- Schema validation on producer (prevents corrupt messages)
- Compatibility enforcement (`BACKWARD`, `FORWARD`, `FULL`)
- Consumer can evolve independently of producer

---

## 4. Kafka Topic Creation Checklist

Before creating a new topic in any environment:

- [ ] Topic name follows `{platform}-{flow}-{role}` convention
- [ ] Partition count = expected max pods × `concurrentConsumers` (e.g. 10×2 = 20)
- [ ] Replication factor = 3 in UAT/prod (fault tolerance)
- [ ] Retention period set (e.g. 7 days for primary, 30 days for DLQ)
- [ ] Consumer group name registered in `kafka.yml`
- [ ] Topic added to `kafka.yml` topic map
- [ ] DLQ topic created alongside primary topic
- [ ] OpenAPI spec updated if topic is documented
- [ ] KEDA ScaledObject created (for consumer routes)

```bash
# Create topic with correct settings
kafka-topics.sh \
  --bootstrap-server 192.168.4.34:9092 \
  --create \
  --topic kraken-new-flow-events \
  --partitions 10 \
  --replication-factor 1 \                   # use 3 in prod
  --config retention.ms=604800000 \          # 7 days
  --config cleanup.policy=delete
```

---

## 5. Kong Gateway Policy Governance

### Rate-limit tuning process

1. Monitor baseline traffic in Grafana (`http_server_requests_seconds_count`)
2. Set limit = 2× measured peak (avoid false throttling)
3. Submit PR with updated `docs/kong.yml`
4. Peer review by integration team lead
5. Deploy via `kong deck sync` in UAT first
6. Monitor for 24h, then promote to prod

### Security review gates

Before any change to `docs/kong.yml`:

- [ ] Rate limits reviewed against traffic baseline
- [ ] CORS `origins` is not `*` in prod (use explicit domains)
- [ ] `request-size-limiting` values validated
- [ ] No new `credentials: true` CORS without security sign-off
- [ ] `security` section reviewed if auth plugins added

---

## 6. Environment Promotion Process

```
dev ──► test ──► uat ──► prod
```

| Config | dev | test | uat | prod |
|--------|-----|------|-----|------|
| Kafka broker | `localhost:9092` | `test-kafka:9092` | `uat-kafka:9092` | `192.168.4.34:9092` |
| SOA-RCDC URL | `localhost:8082` | `mock-rcdc:8082` | `uat-rcdc.pge.com` | `rcdc.pge.com` |
| Logging | console + file | file | file + JSON | JSON only |
| Tracing sampling | 1.0 | 1.0 | 0.5 | 0.1 |
| KEDA | disabled | disabled | enabled | enabled |
| Replicas | 1 | 1 | 2 | 2–10 |

Each environment has its own `config/{env}/application-{env}.yml` which Spring Boot loads via:
```
SPRING_PROFILES_ACTIVE=uat
```

**CI/CD promotion:** See `.github/workflows/ci-cd.yml` — UAT deploys automatically, prod requires manual approval via GitHub environment protection rules.

---

## 7. Integration Lifecycle Management

### Onboarding a new integration

1. **Model** — create request/response models in `models/{domain}/`
2. **Mapper** — create mapper in `mappers/{domain}/`
3. **Processor** — create processor in `processors/{domain}/`
4. **Route** — create HTTP listener or Kafka consumer extending `BaseRoute` / `BaseKafkaConsumerRoute`
5. **Error handling** — `ValidationException` for bad input, `RetryableException` for transient, `ExternalServiceException` for 4xx
6. **Kafka topics** — add to `kafka.yml`, create DLQ + retry topics
7. **OpenAPI** — add endpoint to `docs/openapi.yaml`
8. **Tests** — add integration test with WireMock
9. **Kong** — add route + plugins to `docs/kong.yml`
10. **Docs** — update `docs/demo-guide.md` flow section

### Deprecating an integration

1. Add `Deprecation` header to HTTP response via Kong `response-transformer`
2. Monitor traffic — alert when traffic drops to zero
3. Remove route, processors, models
4. Archive Kafka topic (set retention to 0, then delete after drain)
5. Remove from `docs/kong.yml`, `docs/openapi.yaml`
6. Tag the final version before removal
