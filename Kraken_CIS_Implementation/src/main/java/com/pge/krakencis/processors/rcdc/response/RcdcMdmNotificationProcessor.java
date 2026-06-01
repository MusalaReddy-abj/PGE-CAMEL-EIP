package com.pge.krakencis.processors.rcdc.response;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.mappers.rcdc.response.RcdcMdmNotificationMapper;
import com.pge.krakencis.models.rcdc.response.RcdcHesResponseMessage;
import com.pge.krakencis.processors.BaseProcessor;
import com.pge.krakencis.services.SOAMDMNotificationService;
import com.pge.krakencis.exceptions.RetryableException;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

@Component
public class RcdcMdmNotificationProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(RcdcMdmNotificationProcessor.class);

    private final RcdcMdmNotificationMapper rcdcMdmNotificationMapper;
    private final SOAMDMNotificationService soaMdmNotificationService;
    private final ObjectMapper              objectMapper;

    public RcdcMdmNotificationProcessor(RcdcMdmNotificationMapper rcdcMdmNotificationMapper,
                                         SOAMDMNotificationService soaMdmNotificationService,
                                         ObjectMapper              objectMapper) {
        this.rcdcMdmNotificationMapper = rcdcMdmNotificationMapper;
        this.soaMdmNotificationService  = soaMdmNotificationService;
        this.objectMapper               = objectMapper;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String                 json     = exchange.getIn().getBody(String.class);
        RcdcHesResponseMessage response = objectMapper.readValue(json, RcdcHesResponseMessage.class);

        String correlationId = response.getHeader().getCorrelationID();
        exchange.setProperty(LogConstants.PROP_CORRELATION_ID, correlationId);

        log.info("rcdcMdmNotificationProcessing", correlationId,
            "mRID",      response.getPayload().getDefaultResponse().getEndDeviceAsset().getMRID(),
            "replyCode", response.getReply().getReplyCode());

        String soapXml = rcdcMdmNotificationMapper.toSoapXml(response, correlationId);
        sendNotificationWithRetry(soapXml, correlationId, exchange);
    }

    private void sendNotificationWithRetry(String soapXml, String correlationId, Exchange exchange) throws Exception {
        final int maxRetries = 3;
        int attempt = 0;
        int statusCode = 0;
        Exception lastException = null;

        while (attempt < maxRetries) {
            try {
                attempt++;
                statusCode = soaMdmNotificationService.sendNotification(soapXml, correlationId);
                exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, statusCode);

                // Success - 2xx status code
                if (isSuccessStatusCode(statusCode)) {
                    log.info("notificationSentSuccessfully", correlationId, "statusCode", statusCode, "attempt", attempt);
                    return;
                }

                // Non-2xx status code - check if retryable
                if (isRetryableStatusCode(statusCode)) {
                    if (attempt < maxRetries) {
                        long backoffTime = calculateExponentialBackoff(attempt);
                        log.warn("retryableStatusCodeReceived", correlationId,
                            "statusCode", statusCode, "attempt", attempt, "maxRetries", maxRetries,
                            "nextRetryInMs", backoffTime);
                        Thread.sleep(backoffTime);
                        continue;
                    } else {
                        // Max retries exhausted - route to retry queue
                        log.error("maxRetriesExhaustedForRetryableError", correlationId,
                            "statusCode", statusCode, "attempts", attempt);
                        routeToRetryQueue(exchange, soapXml, correlationId, statusCode);
                        return;
                    }
                } else {
                    // Non-retryable status code (4xx except 408/429) - route to DLQ
                    log.error("nonRetryableStatusCodeReceived", correlationId, "statusCode", statusCode);
                    routeToDLQ(exchange, soapXml, correlationId, statusCode);
                    return;
                }

            } catch (Exception e) {
                lastException = e;
                
                // Check if this is a retryable exception type
                if (isRetryableException(e)) {
                    if (attempt < maxRetries) {
                        long backoffTime = calculateExponentialBackoff(attempt);
                        log.warn("retryableExceptionOnAttempt", correlationId,
                            "exceptionType", e.getClass().getSimpleName(), "attempt", attempt,
                            "maxRetries", maxRetries, "nextRetryInMs", backoffTime, "error", e.getMessage());
                        Thread.sleep(backoffTime);
                    } else {
                        // Max retries exhausted for connection error - route to retry queue
                        log.error("maxRetriesExhaustedForConnectionError", correlationId,
                            "exceptionType", e.getClass().getSimpleName(), "attempts", attempt, "error", e.getMessage());
                        routeToRetryQueue(exchange, soapXml, correlationId, -1);
                        return;
                    }
                } else {
                    // Other non-retryable exceptions - route to DLQ
                    log.error("nonRetryableExceptionReceived", correlationId,
                        "exceptionType", e.getClass().getSimpleName(), "error", e.getMessage());
                    routeToDLQ(exchange, soapXml, correlationId, -1);
                    return;
                }
            }
        }

        // Should not reach here, but if we do, it's an error state
        String msg = "Unexpected error state after notification retry attempts";
        log.error("unexpectedErrorStateInRetry", correlationId, "message", msg);
        if (lastException != null) {
            throw lastException;
        }
        throw new RuntimeException(msg);
    }

    /**
     * Checks if the HTTP status code represents a successful response (2xx)
     */
    private boolean isSuccessStatusCode(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    /**
     * Determines if an HTTP status code is retryable.
     * Retryable: 5xx errors, 408 (Request Timeout), 429 (Too Many Requests)
     */
    private boolean isRetryableStatusCode(int statusCode) {
        return statusCode >= 500 || statusCode == 408 || statusCode == 429;
    }

    /**
     * Calculates exponential backoff with jitter.
     * Attempt 1: 1s, Attempt 2: 2s, Attempt 3: 4s, etc.
     */
    private long calculateExponentialBackoff(int attempt) {
        long baseDelay = (long) Math.pow(2, attempt - 1) * 1000; // 2^(attempt-1) seconds
        long jitter = (long) (Math.random() * 1000); // Add up to 1s random jitter
        return baseDelay + jitter;
    }

    /**
     * Checks if an exception is retryable (transient error).
     * Retryable: connection errors, timeouts, network issues
     * Non-retryable: parsing errors, validation errors, business logic errors
     */
    private boolean isRetryableException(Exception e) {
        return e.getCause() instanceof java.net.ConnectException
            || e.getCause() instanceof java.net.SocketTimeoutException
            || e.getCause() instanceof java.net.UnknownHostException
            || e.getCause() instanceof java.io.InterruptedIOException
            || e instanceof RetryableException
            || (e.getMessage() != null && 
                (e.getMessage().contains("Connection refused")
                 || e.getMessage().contains("timeout")
                 || e.getMessage().contains("Temporary failure")));
    }

    /**
     * Routes failed message to retry queue for later reprocessing.
     * Used for transient/retryable failures after max retries exhausted.
     */
    private void routeToRetryQueue(Exchange exchange, String soapXml, String correlationId, int statusCode) {
        log.info("routingMessageToRetryQueue", correlationId, "statusCode", statusCode);
        exchange.getIn().setBody(soapXml);
        exchange.getIn().setHeader("X-Correlation-ID", correlationId);
        exchange.getIn().setHeader("X-Original-Status-Code", statusCode);
        exchange.getIn().setHeader("X-Retry-Reason", "MAX_RETRIES_EXHAUSTED");
        exchange.getIn().setHeader("X-Route-Destination", "retry-queue");
    }

    /**
     * Routes failed message to Dead Letter Queue.
     * Used for non-retryable failures (4xx client errors, validation errors, etc.)
     */
    private void routeToDLQ(Exchange exchange, String soapXml, String correlationId, int statusCode) {
        log.error("routingMessageToDLQ", correlationId, "statusCode", statusCode);
        exchange.getIn().setBody(soapXml);
        exchange.getIn().setHeader("X-Correlation-ID", correlationId);
        exchange.getIn().setHeader("X-Original-Status-Code", statusCode);
        exchange.getIn().setHeader("X-DLQ-Reason", statusCode >= 400 && statusCode < 500 ? 
            "CLIENT_ERROR_NON_RETRYABLE" : "UNEXPECTED_ERROR");
        exchange.getIn().setHeader("X-Route-Destination", "dlq");
    }
}
