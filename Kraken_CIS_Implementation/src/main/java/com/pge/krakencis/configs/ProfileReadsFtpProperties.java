package com.pge.krakencis.configs;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "ftp.profile-reads")
public class ProfileReadsFtpProperties {

    private FtpCommonProperties connection          = new FtpCommonProperties();
    private String             remoteDirectory     = "/incoming";
    private String             moveDirectory       = ".done";
    private String             errorDirectory      = ".error";
    private String             include             = ".*\\.csv";
    private int                delayMs             = 60000;

    /** Must be false for text CSV files so charset is handled correctly. */
    private boolean            binary              = false;
    private String             charset             = "UTF-8";
    private boolean            passiveMode         = true;
    private String             readLock            = "changed";
    private long               readLockMinAgeMs    = 1000;

    /** Max files consumed per poll cycle — prevents OOM on large FTP backlogs. */
    private int                maxMessagesPerPoll  = 50;

    /**
     * Number of threads used to process downloaded files in parallel inside the route.
     * Applied via {@code .threads(n)} in the route — NOT passed to the FTP URI.
     * The FTP consumer itself always polls sequentially; this controls downstream parallelism.
     */
    private int                threadPoolSize      = 4;

    /** Idempotent file-name repository to prevent duplicate processing on re-upload. */
    private boolean            idempotent          = true;

    public String buildUri() {
        return connection.buildBaseUri() + normalizeDirectory(remoteDirectory)
            + "?username=" + connection.getUsername()
            + "&password=" + connection.getPassword()
            + "&binary=" + binary
            + "&charset=" + charset
            + "&passiveMode=" + passiveMode
            + "&include=" + include
            + "&delay=" + delayMs
            + "&maxMessagesPerPoll=" + maxMessagesPerPoll
            + "&idempotent=" + idempotent
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
