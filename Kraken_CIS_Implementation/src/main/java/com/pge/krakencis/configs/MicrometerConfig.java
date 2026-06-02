package com.pge.krakencis.configs;

// Requires this dependency in pom.xml (version managed by Spring Boot BOM):
//
// <dependency>
//     <groupId>io.micrometer</groupId>
//     <artifactId>micrometer-registry-prometheus</artifactId>
// </dependency>

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Micrometer / Prometheus metrics configuration.
 *
 * <p>Applies {@code application=kraken-cis} and {@code version=1.0.0} common tags
 * to every meter so all metrics exported to {@code /actuator/prometheus} carry
 * consistent identifying labels for Prometheus scraping and Grafana dashboards.
 *
 * <h3>Exposed metrics (sample)</h3>
 * <ul>
 *   <li>{@code camel_exchanges_total}        — exchange count per route</li>
 *   <li>{@code camel_exchanges_failed_total} — failed exchange count per route</li>
 *   <li>{@code kafka_consumer_records_lag}   — consumer group lag per partition</li>
 *   <li>{@code jvm_memory_used_bytes}        — JVM heap / non-heap</li>
 * </ul>
 *
 * <h3>Scrape endpoint</h3>
 * {@code GET http://localhost:9091/actuator/prometheus}
 */
@Configuration
public class MicrometerConfig {

    @Bean
    public MeterRegistryCustomizer<MeterRegistry> prometheusCommonTags() {
        return registry -> registry.config()
            .commonTags(
                "application", "kraken-cis",
                "version",     "1.0.0"
            );
    }
}
