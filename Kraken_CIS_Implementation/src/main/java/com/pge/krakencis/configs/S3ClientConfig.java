package com.pge.krakencis.configs;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/**
 * Creates the {@link S3Client} bean used by
 * {@link com.pge.krakencis.routes.profilereads.ProfileReadsS3Listner}
 * to move files between S3 prefixes (archive / error) after processing.
 *
 * <p>Only instantiated when {@code aws.s3.profile-reads.bucket-name} is set,
 * so the S3 listener and its dependencies are completely absent when the
 * S3 feature is not configured.
 *
 * <h3>Credential resolution order</h3>
 * <ol>
 *   <li>If {@code aws.s3.profile-reads.connection.use-default-credentials=true},
 *       the AWS Default Credentials Provider chain is used
 *       (IAM role / env vars / ~/.aws/credentials).</li>
 *   <li>Otherwise, explicit access-key / secret-key from {@code s3.yml} are used.</li>
 * </ol>
 */
@Configuration
@ConditionalOnProperty(prefix = "aws.s3.profile-reads", name = "bucket-name")
public class S3ClientConfig {

    @Value("${aws.s3.profile-reads.connection.region:us-east-1}")
    private String region;

    @Value("${aws.s3.profile-reads.connection.access-key:}")
    private String accessKey;

    @Value("${aws.s3.profile-reads.connection.secret-key:}")
    private String secretKey;

    @Value("${aws.s3.profile-reads.connection.use-default-credentials:false}")
    private boolean useDefaultCredentials;

    @Bean
    @ConditionalOnMissingBean(S3Client.class)
    public S3Client s3Client() {
        S3ClientBuilder builder = S3Client.builder()
            .region(Region.of(region));

        if (useDefaultCredentials) {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        } else {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKey, secretKey)));
        }

        return builder.build();
    }
}
