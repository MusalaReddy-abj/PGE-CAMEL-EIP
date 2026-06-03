package com.pge.krakencis.health;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Spring Boot health indicator that checks Kafka broker connectivity.
 *
 * <p>Exposed at {@code GET /actuator/health/kafka} (management port 9091).
 * Uses a short-lived {@link AdminClient} to verify the broker is reachable
 * and can list topics within 5 seconds.
 *
 * <h3>Healthy response</h3>
 * <pre>
 * {
 *   "status": "UP",
 *   "details": { "brokers": "192.168.4.34:9092" }
 * }
 * </pre>
 *
 * <h3>Unhealthy response</h3>
 * <pre>
 * {
 *   "status": "DOWN",
 *   "details": { "error": "Connection refused: 192.168.4.34:9092" }
 * }
 * </pre>
 */
@Component("kafka")
public class KafkaHealthIndicator implements HealthIndicator {

    @Value("${kafka.producer.brokers}")
    private String bootstrapServers;

    @Override
    public Health health() {
        try (AdminClient adminClient = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                       AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000",
                       AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000"))) {

            adminClient.listTopics()
                       .names()
                       .get(5, TimeUnit.SECONDS);

            return Health.up()
                         .withDetail("brokers", bootstrapServers)
                         .build();

        } catch (Exception ex) {
            return Health.down()
                         .withDetail("brokers", bootstrapServers)
                         .withDetail("error", ex.getMessage())
                         .build();
        }
    }
}
