package com.pge.krakencis.configs;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Centralised registry of all outbound external service endpoints.
 *
 * Configure in application.yml (or env-specific overrides):
 *
 *   external-services:
 *     services:
 *       rcdc:
 *         url: http://soa-host/api/rcdc/command
 *         timeout: 30000
 *         content-type: application/json
 *       some-soap-service:
 *         url: http://soap-host/ws/endpoint
 *         timeout: 60000
 *         content-type: text/xml; charset=utf-8
 *
 * Inject ExternalServiceProperties into any service class that needs
 * to call an external endpoint — never use @Value for target URLs.
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "external-services")
public class ExternalServiceProperties {

    private Map<String, ServiceEndpoint> services = new HashMap<>();

    @Data
    public static class ServiceEndpoint {
        private String url;
        private int    timeout     = 30_000;
        private String contentType = "application/json";
    }

    /**
     * Looks up a registered service endpoint by its key name.
     * Throws a clear startup error if the key is missing from properties.
     */
    public ServiceEndpoint get(String serviceKey) {
        ServiceEndpoint endpoint = services.get(serviceKey);
        if (endpoint == null) {
            throw new IllegalStateException(
                "No external-services.services entry found for key '" + serviceKey
                + "'. Add it to application.yml or the active profile config.");
        }
        return endpoint;
    }
}
