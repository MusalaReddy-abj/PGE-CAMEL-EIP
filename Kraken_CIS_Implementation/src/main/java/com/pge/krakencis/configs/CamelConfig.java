package com.pge.krakencis.configs;

import org.apache.camel.CamelContext;
import org.apache.camel.spring.boot.CamelContextConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CamelConfig {

    @Bean
    CamelContextConfiguration camelContextConfiguration() {
        return new CamelContextConfiguration() {
            @Override
            public void beforeApplicationStart(CamelContext context) {
                context.setStreamCaching(true);
                context.getGlobalOptions().put("CamelJacksonEnableTypeConverter", "true");
            }

            @Override
            public void afterApplicationStart(CamelContext context) {
                // post-start hooks
            }
        };
    }
}
