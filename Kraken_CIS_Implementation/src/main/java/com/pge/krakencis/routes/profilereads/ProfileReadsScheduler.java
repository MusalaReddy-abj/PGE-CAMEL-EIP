package com.pge.krakencis.routes.profilereads;

import com.pge.krakencis.configs.ProfileReadsS3Properties;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.models.profilereads.ProfileReadsWorkItem;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.ArrayList;
import java.util.List;

/**
 * Profile Reads — work-queue <b>scheduler</b> (discovery half of the work-queue pattern).
 *
 * <p>On a timer it lists {@code .csv} files under the S3 source prefix and publishes one
 * lightweight work-item per file ({@link ProfileReadsWorkItem}) to the work topic, <b>keyed
 * by fileName</b>. The heavy work (download + parse + publish + archive) is done by
 * {@code ProfileReadsWorkConsumer}, distributed across pods by the consumer group — so each
 * file is processed by exactly one pod, while both pods stay busy across different files.
 *
 * <p>Runs on <b>every</b> pod. Both pods may publish a work-item for the same file; that is
 * harmless because (a) the key=fileName routes all of a file's work-items to one consumer and
 * (b) the consumer is idempotent (skips once the file is archived). This is the cross-pod
 * duplicate fix: no leader election, no extra infra — Kafka's consumer group is the
 * coordinator.
 *
 * <p>Active only when an S3 bucket is configured AND
 * {@code profile-reads.ingestion.mode=work-queue} (the default). Set the mode to
 * {@code poller} to fall back to the direct {@code aws2-s3} polling route.
 */
@Component
@ConditionalOnExpression(
    "'${aws.s3.profile-reads.bucket-name:}' != '' and '${profile-reads.ingestion.mode:work-queue}' == 'work-queue'")
public class ProfileReadsScheduler extends RouteBuilder {

    private static final StructuredLogger log = StructuredLogger.of(ProfileReadsScheduler.class);

    private final ProfileReadsS3Properties s3Properties;
    private final S3Client                 s3Client;

    @Value("${kafka.topic.profile-reads-work:kraken-profile-reads-work-events}")
    private String workTopic;

    public ProfileReadsScheduler(ProfileReadsS3Properties s3Properties, S3Client s3Client) {
        this.s3Properties = s3Properties;
        this.s3Client     = s3Client;
    }

    @Override
    public void configure() {
        from("timer:profile-reads-scan?period=" + s3Properties.getDelayMs() + "&delay=5000")
            .routeId("route-profile-reads-scheduler")
            .process(this::listWorkItems)
            .split(body())
                .process(this::prepareWorkItemPublish)
                .to("direct:publishToKafka")     // reuses existing publish (key, marshal, tracing)
            .end();
    }

    /** Lists {@code .csv} objects under the source prefix and sets the body to a list of work-items. */
    private void listWorkItems(Exchange exchange) {
        String bucket = s3Properties.getBucketName();
        String prefix = s3Properties.getSourcePrefix();

        ListObjectsV2Response resp = s3Client.listObjectsV2(ListObjectsV2Request.builder()
            .bucket(bucket)
            .prefix(prefix)
            .build());

        List<ProfileReadsWorkItem> items = new ArrayList<>();
        for (S3Object obj : resp.contents()) {
            String key = obj.key();
            // Only real .csv files — skip the folder/.keep placeholder objects.
            if (key.toLowerCase().endsWith(".csv")) {
                items.add(new ProfileReadsWorkItem(bucket, key, fileNameFrom(key)));
            }
        }

        exchange.getIn().setBody(items);
        if (!items.isEmpty()) {
            log.info("profileReadsScanned", null, "bucket", bucket, "prefix", prefix, "files", items.size());
        }
    }

    /** Sets the topic + per-item key (fileName) + correlation id before publishing one work-item. */
    private void prepareWorkItemPublish(Exchange exchange) {
        ProfileReadsWorkItem item = exchange.getIn().getBody(ProfileReadsWorkItem.class);
        exchange.setProperty(LogConstants.KAFKA_TOPIC,        workTopic);
        exchange.setProperty(LogConstants.KAFKA_KEY,          item.fileName());   // duplicates → same partition
        exchange.setProperty(LogConstants.PROP_CORRELATION_ID, item.fileName());  // trace by file
    }

    private static String fileNameFrom(String s3Key) {
        if (s3Key == null) return "unknown.csv";
        int lastSlash = s3Key.lastIndexOf('/');
        return lastSlash >= 0 ? s3Key.substring(lastSlash + 1) : s3Key;
    }
}
