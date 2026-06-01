# RcdcMdmNotificationProcessor - Retry & Error Handling Implementation

## What's Now Implemented ✅

### 1. Retry Mechanism
- **Max retries:** 3 attempts
- **Backoff strategy:** Exponential with jitter (2^(n-1) seconds + 0-1s random)
- **Retryable errors:**
  - HTTP 5xx (server errors)
  - HTTP 408 (Request Timeout)
  - HTTP 429 (Too Many Requests)
  - Connection exceptions (ConnectException, SocketTimeoutException, UnknownHostException)
  - InterruptedIOException
  - Custom RetryableException

### 2. Error Classification
```
HTTP Status Code Response
    ├── 2xx (Success) → Log & return
    ├── 5xx/408/429 (Retryable) → Retry 3x, then → Retry Queue
    └── 4xx except 408/429 (Non-retryable) → DLQ

Exception Type Response
    ├── Connection/Network Errors (Retryable) → Retry 3x, then → Retry Queue
    └── Parse/Validation/Other (Non-retryable) → DLQ
```

### 3. Dead-Letter Queue (DLQ) Routing
- **Trigger:** Non-retryable errors or max retries exhausted on connection errors
- **Data preserved:** 
  - Original SOAP XML
  - Correlation ID (tracing)
  - Original HTTP status code or exception type
  - Failure reason classification

### 4. Retry Queue Routing
- **Trigger:** Retryable errors after max retries
- **Data preserved:** Same as DLQ
- **Intended consumer:** Retry queue consumer (NOT YET IMPLEMENTED - see below)

### 5. Structured Logging & Observability
- **Log format:** Structured JSON-compatible logs
- **Correlation tracking:** Preserved across retries and route changes
- **Log events:**
  - `notificationSentSuccessfully` - Success case with status and attempt count
  - `retryableStatusCodeReceived` - Will retry
  - `maxRetriesExhaustedForRetryableError` - Retry exhausted → DLQ
  - `retryableExceptionOnAttempt` - Exception caught, will retry
  - `maxRetriesExhaustedForConnectionError` - Connection retry exhausted → Retry Queue
  - `nonRetryableExceptionReceived` - Fatal error → DLQ
  - `routingMessageToRetryQueue` - Moving to retry queue
  - `routingMessageToDLQ` - Moving to DLQ

---

## What's Missing (Must Implement) 🚨

### 1. **CRITICAL:** Retry Queue Consumer
The code routes messages to `kraken-mdm-notification-retry-queue` but **no consumer exists** to process them.

**Impact:** Messages move to retry queue but never get reprocessed. Notifications stuck.

**Solution Required:**
```java
@Component
public class RetryQueueConsumer extends RouteBuilder {
    
    @Override
    public void configure() throws Exception {
        from("kafka:kraken-mdm-notification-retry-queue?groupId=retry-consumer")
            .log("Reprocessing retry queue message: ${header.X-Correlation-ID}")
            .setHeader("X-Retry-Count", simple("${header.X-Retry-Count:0} + 1"))
            .process(exchange -> {
                // Re-deserialize and reprocess
                String soapXml = exchange.getIn().getBody(String.class);
                String correlationId = exchange.getIn().getHeader("X-Correlation-ID", String.class);
                // Call notification service again...
            })
            .choice()
                .when().simple("${header.X-Retry-Count} >= 3")
                    .to("kafka:kraken-mdm-notification-dlq")  // Move to DLQ after 3 retries
                    .log("Retry exhausted, moving to DLQ: ${header.X-Correlation-ID}")
                .otherwise()
                    .log("Retrying message: ${header.X-Correlation-ID}")
                    .to("bean:rcdcMdmNotificationProcessor")
                .end();
    }
}
```

### 2. **CRITICAL:** DLQ Consumer / Monitoring
Messages route to DLQ but **no visibility or replay mechanism** exists.

**Impact:** Messages disappear into DLQ with no way to inspect or recover them.

**Solution Required:**
```java
@RestController
@RequestMapping("/api/dlq")
public class DLQController {
    
    @GetMapping("/messages")
    public List<DLQMessage> listDLQMessages(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        // Fetch DLQ messages from Kafka topic
    }
    
    @PostMapping("/replay")
    public void replayMessage(@RequestParam String correlationId) {
        // Fetch from DLQ, validate, and reprocess
    }
    
    @GetMapping("/stats")
    public DLQStats getDLQStats() {
        // Error counts by type, date, endpoint
    }
}
```

### 3. **IMPORTANT:** Circuit Breaker
Currently **retries immediately** if service is down. Should fail fast and degrade gracefully.

**Impact:** Hammering a down service with 3 retries per message = wasted latency.

**Solution Required:**
```java
@Configuration
public class CircuitBreakerConfig {
    
    @Bean
    public io.github.resilience4j.circuitbreaker.CircuitBreaker notificationCB() {
        return CircuitBreakerRegistry.ofDefaults()
            .circuitBreaker("notification-service",
                CircuitBreakerConfig.custom()
                    .failureRateThreshold(50.0f)      // Open after 50% failures
                    .waitDurationInOpenState(Duration.ofSeconds(30))
                    .permittedNumberOfCallsInHalfOpenState(3)
                    .build()
            );
    }
}

// Usage in processor:
@CircuitBreaker(name = "notification-service", fallbackMethod = "notificationFallback")
private int sendNotificationWithCircuitBreaker(String soapXml, String correlationId) {
    return soaMdmNotificationService.sendNotification(soapXml, correlationId);
}

private int notificationFallback(String soapXml, String correlationId, Exception e) {
    log.error("Circuit breaker open for notifications", correlationId);
    // Route to retry queue immediately
    return 503;  // Service Unavailable
}
```

### 4. **IMPORTANT:** Timeout Handling
No explicit timeout on HTTP call. If MDM service hangs, will block indefinitely.

**Impact:** Thread pool exhaustion if multiple calls hang.

**Solution:**
```java
// In SOAMDMNotificationService
HttpClient client = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(5))
    .build();

HttpRequest request = HttpRequest.newBuilder()
    .uri(URI.create(mdmServiceUrl))
    .timeout(Duration.ofSeconds(10))  // 10s total timeout
    .POST(HttpRequest.BodyPublishers.ofString(soapXml))
    .build();
```

### 5. **IMPORTANT:** Metrics/Observability
No metrics exported (Prometheus/Grafana). Impossible to monitor in production.

**Solution:**
```java
@Configuration
public class MetricsConfig {
    
    @Bean
    MeterRegistryCustomizer<MeterRegistry> metricsCustomizer() {
        return registry -> {
            registry.counter("kraken.notification.attempts.total", 
                "status", "success");  // per attempt status
            registry.timer("kraken.notification.duration",
                "service", "mdm");  // latency tracking
            registry.gauge("kraken.dlq.size");  // DLQ backlog
        };
    }
}

// Usage in processor:
MeterRegistry meterRegistry;
meterRegistry.recordTime("kraken.notification.duration", 
    () -> soaMdmNotificationService.sendNotification(soapXml, correlationId));
```

---

## What's Partially Implemented (Needs Enhancement) ⚠️

### 1. **DLQ/Retry Queue Routing**
- ✅ Headers set correctly
- ❌ Not using Camel's built-in retry/error handler
- ❌ Should use `.onException()` DSL instead of try/catch

**Better approach:**
```java
@Component
public class NotificationRouteWithErrorHandling extends RouteBuilder {
    
    @Override
    public void configure() throws Exception {
        onException(RetryableException.class)
            .retryAttemptedLogLevel(LoggingLevel.WARN)
            .maximumRedeliveries(3)
            .redeliveryDelay(1000)
            .backOffMultiplier(2.0)
            .useExponentialBackOff()
            .to("kafka:kraken-mdm-notification-retry-queue");
        
        onException(Exception.class)
            .logExhaustedMessageHistory(true)
            .maximumRedeliveries(0)
            .to("kafka:kraken-mdm-notification-dlq");
            
        from("kafka:kraken-rcdc-hes-response-events")
            .process(new RcdcMdmNotificationProcessor(...))
            .to("bean:mdmNotificationService");
    }
}
```

### 2. **Error Visibility**
- ✅ Logs written
- ❌ No centralized error aggregation
- ❌ No dashboard showing error trends

---

## Deployment Checklist 📋

Before deploying to production:

- [ ] Implement Retry Queue Consumer
- [ ] Implement DLQ monitoring endpoint & UI
- [ ] Add Circuit Breaker for MDM service
- [ ] Add timeout on MDM HTTP call (10s)
- [ ] Add Prometheus metrics export
- [ ] Add alerting rules for:
  - [ ] DLQ size > 1000
  - [ ] Retry queue size growing
  - [ ] Circuit breaker open
  - [ ] Error rate spike
- [ ] Load test with 1000 req/min sustained
- [ ] Chaos test: Kill MDM service, verify graceful degradation
- [ ] Verify correlation IDs flow through all logs
- [ ] Document retry policy in API contract
- [ ] Create runbook for DLQ recovery

---

## Testing Your Implementation 🧪

```bash
# 1. Test success path
curl -X POST http://localhost:8080/api/rcdc \
  -H "Content-Type: application/json" \
  -d '{...valid request...}'

# 2. Test retry (simulate delay)
# Stop MDM service: docker stop mdm-service
# Send request: curl -X POST http://localhost:8080/api/rcdc ...
# Monitor logs: grep "retryableStatusCodeReceived" logs/app.log
# Start MDM: docker start mdm-service
# Verify retry succeeds

# 3. Test DLQ routing
# Cause validation error: curl -X POST http://localhost:8080/api/rcdc \
#   -d '{...invalid payload...}'
# Check DLQ: kafka-console-consumer --topic kraken-mdm-notification-dlq

# 4. Verify correlation ID
# grep "X-Correlation-ID" logs/app.log
# Should see same ID across request → processor → service → dlq/retry
```

---

## Summary

| Component | Status | Action |
|-----------|--------|--------|
| Retry logic (3x) | ✅ DONE | Deploy as-is |
| Exponential backoff | ✅ DONE | Deploy as-is |
| DLQ routing | ✅ Headers set | Need consumer |
| Retry queue routing | ✅ Headers set | Need consumer |
| Circuit breaker | ❌ MISSING | Must add |
| Timeout handling | ❌ MISSING | Must add |
| Metrics | ❌ MISSING | Should add |
| DLQ visibility | ❌ MISSING | Should add |
| Retry consumer | ❌ MISSING | Critical blocker |

**Recommendation:** Do NOT deploy to production without implementing:
1. Retry Queue Consumer (critical: no message recovery)
2. DLQ visibility endpoint (critical: no troubleshooting)
3. Timeout handling (important: risk of thread starvation)
