package com.pge.krakencis.configs;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * AWS S3 configuration for the Profile Reads S3 polling route.
 *
 * Mirrors {@link ProfileReadsFtpProperties} — same fields, same purpose,
 * different transport. Both feed into the same
 * {@link com.pge.krakencis.processors.profilereads.ProfileReadsCsvProcessor}.
 *
 * <pre>
 * aws:
 *   s3:
 *     profile-reads:
 *       connection:
 *         region:                    us-east-1
 *         access-key:                AKIAIOSFODNN7EXAMPLE   # or leave blank for IAM role
 *         secret-key:                wJalrXUtnFEMI...
 *         use-default-credentials:   false
 *       bucket-name:                 kraken-profile-reads
 *       source-prefix:               incoming/
 *       archive-prefix:              archive/
 *       error-prefix:                error/
 *       delay-ms:                    60000
 *       max-messages-per-poll:       50
 *       thread-pool-size:            4
 * </pre>
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "aws.s3.profile-reads")
public class ProfileReadsS3Properties {

    private S3ConnectionProperties connection         = new S3ConnectionProperties();
    private String                 bucketName         = "kraken-profile-reads";
    private String                 sourcePrefix       = "incoming/";
    private String                 archivePrefix      = "archive/";
    private String                 errorPrefix        = "error/";
    private int                    delayMs            = 60_000;
    private int                    maxMessagesPerPoll = 50;
    private int                    threadPoolSize     = 4;
    /** Maximum allowed file size in MB. Files exceeding this are rejected before parsing. */
    private int                    maxFileSizeMb      = 100;
    /** CSV rows per Kafka publish batch — bounds in-process memory regardless of file size. */
    private int                    kafkaBatchSize     = 500;

    @Data
    public static class S3ConnectionProperties {
        private String  region                 = "us-east-1";
        private String  accessKey              = "";
        private String  secretKey              = "";
        private boolean useDefaultCredentials  = false;
    }

    /**
     * Builds the Camel AWS2-S3 consumer URI.
     *
     * <p>Credentials are supplied at the <em>Camel component level</em>
     * ({@code camel.component.aws2-s3.*}) rather than embedded in the URI,
     * so they never appear in logs or stack traces.
     */
    public String buildUri() {
        StringBuilder uri = new StringBuilder("aws2-s3://")
            .append(bucketName)
            .append("?region=").append(connection.getRegion())
            .append("&deleteAfterRead=false")          // we manage lifecycle manually
            .append("&moveAfterRead=false")
            .append("&includeBody=true")               // stream file content into exchange body
            .append("&autocloseBody=false")            // ProfileReadsCsvProcessor closes stream via try-with-resources
            .append("&maxMessagesPerPoll=").append(maxMessagesPerPoll)
            .append("&delay=").append(delayMs)
            .append("&sendEmptyMessageWhenIdle=false");

        if (sourcePrefix != null && !sourcePrefix.isBlank()) {
            uri.append("&prefix=").append(sourcePrefix);
        }

        if (connection.isUseDefaultCredentials()) {
            uri.append("&useDefaultCredentialsProvider=true");
        }

        return uri.toString();
    }
}
