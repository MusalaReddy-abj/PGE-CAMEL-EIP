package com.pge.krakencis.exceptions;

/**
 * Thrown when a message has exceeded the maximum number of retry-queue
 * re-attempts and must be routed to the final Dead Letter Queue.
 *
 * <p>Caught by the {@code onException(Exception.class)} handler in
 * {@link com.pge.krakencis.routes.BaseKafkaConsumerRoute} which routes
 * the message to the DLQ topic and commits the Kafka offset.
 */
public class RetryQueueExhaustedException extends RuntimeException {

    private final int attempts;

    public RetryQueueExhaustedException(String serviceName, int attempts) {
        super(serviceName + " retry queue exhausted after " + attempts + " attempt(s)");
        this.attempts = attempts;
    }

    public int getAttempts() {
        return attempts;
    }
}
