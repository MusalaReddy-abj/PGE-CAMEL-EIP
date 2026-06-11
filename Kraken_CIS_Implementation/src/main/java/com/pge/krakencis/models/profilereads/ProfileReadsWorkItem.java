package com.pge.krakencis.models.profilereads;

/**
 * A unit of work published by {@code ProfileReadsScheduler} and consumed by
 * {@code ProfileReadsWorkConsumer}: it points at one S3 file to process.
 *
 * <p>Discovery (listing S3) is decoupled from processing (download + parse + publish) via a
 * Kafka work topic. The consumer group hands each work-item to exactly one pod, so a file is
 * processed by only one pod even though both pods run the scheduler. The message is keyed by
 * {@code fileName} so any duplicate work-items for the same file land on the same partition
 * (one consumer), and the consumer is idempotent (skips when the file is already archived).
 */
public record ProfileReadsWorkItem(String bucket, String key, String fileName) {
}
