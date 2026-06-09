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

    /**
     * Maximum allowed file size in megabytes. Files exceeding this are rejected
     * before parsing starts — prevents OOM from unexpectedly large uploads.
     */
    private int                maxFileSizeMb       = 100;

    /**
     * Number of parsed CSV rows published to Kafka per batch.
     * Bounds memory usage to {@code kafkaBatchSize × sizeof(KafkaProfileReadPayload)}
     * regardless of how many rows the file contains.
     */
    private int                kafkaBatchSize      = 500;

    public String buildUri() {
        // noop=true — Camel leaves the remote file untouched after the exchange completes.
        // The route performs the archive/error move manually via FTPClient so that a move
        // failure can be caught and logged without failing the exchange. With the old
        // move=.done approach, a post-processing move failure caused Camel to roll back the
        // idempotent entry, making the file eligible for reprocessing on the next poll
        // (duplicate Kafka messages). noop=true prevents that rollback entirely.
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
            + "&noop=true"
            + "&readLock=" + readLock
            + "&readLockMinAge=" + readLockMinAgeMs;
    }

    private String normalizeDirectory(String directory) {
        if (directory == null || directory.isBlank()) {
            return "";
        }
        return directory.startsWith("/") ? directory : "/" + directory;
    }

}
