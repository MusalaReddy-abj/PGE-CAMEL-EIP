package com.pge.krakencis.configs;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "ftp.profile-reads")
public class ProfileReadsFtpProperties {

    private FtpCommonProperties connection = new FtpCommonProperties();
    private String             remoteDirectory = "/incoming";
    private String             moveDirectory   = ".done";
    private String             errorDirectory  = ".error";
    private String             include         = ".*\\.csv";
    private int                delayMs         = 60000;
    private boolean            binary          = true;
    private boolean            passiveMode     = true;
    private String             readLock        = "changed";
    private long               readLockMinAgeMs = 1000;

    public String buildUri() {
        return connection.buildBaseUri() + normalizeDirectory(remoteDirectory)
            + "?username=" + connection.getUsername()
            + "&password=" + connection.getPassword()
            + "&binary=" + binary
            + "&passiveMode=" + passiveMode
            + "&include=" + include
            + "&delay=" + delayMs
            + "&move=" + normalizeMovePath(moveDirectory)
            + "&readLock=" + readLock
            + "&readLockMinAge=" + readLockMinAgeMs
            + "&moveFailed=" + normalizeMovePath(errorDirectory);
    }

    private String normalizeDirectory(String directory) {
        if (directory == null || directory.isBlank()) {
            return "";
        }
        return directory.startsWith("/") ? directory : "/" + directory;
    }

    private String normalizeMovePath(String movePath) {
        if (movePath == null || movePath.isBlank()) {
            return ".done";
        }
        return movePath;
    }

    public String normalizeErrorDirectory(String errorPath) {
        if (errorPath == null || errorPath.isBlank()) {
            return ".error";
        }
        return errorPath.startsWith("/") ? errorPath : "/" + errorPath;
    }

    public String buildErrorUri() {
        return connection.buildBaseUri() + normalizeErrorDirectory(errorDirectory);
    }
}
