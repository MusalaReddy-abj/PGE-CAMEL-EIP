package com.pge.krakencis.configs;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "ftp.common")
public class FtpCommonProperties {

    private String host     = "localhost";
    private int    port     = 21;
    private String username = "anonymous";
    private String password = "anonymous";

    public String buildBaseUri() {
        return "ftp://" + host + ":" + port;
    }
}
