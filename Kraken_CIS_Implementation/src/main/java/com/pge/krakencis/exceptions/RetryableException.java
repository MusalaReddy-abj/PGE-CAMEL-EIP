package com.pge.krakencis.exceptions;

/**
 * Signals a transient failure that Camel's redelivery policy should retry.
 */
public class RetryableException extends KrakenBaseException {

    private final int maxRetries;

    public RetryableException(ErrorCode errorCode, String message,
                               String correlationId, int maxRetries) {
        super(errorCode, ErrorCategory.INTEGRATION, message, correlationId);
        this.maxRetries = maxRetries;
        withContext("maxRetries", maxRetries);
    }

    public RetryableException(ErrorCode errorCode, String message,
                               String correlationId, int maxRetries, Throwable cause) {
        super(errorCode, ErrorCategory.INTEGRATION, message, correlationId, cause);
        this.maxRetries = maxRetries;
        withContext("maxRetries", maxRetries);
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public static RetryableException transient_(String message, String correlationId) {
        return new RetryableException(ErrorCode.TRANSIENT_ERROR, message, correlationId, 3);
    }

    public static RetryableException networkError(String message, String correlationId,
                                                   Throwable cause) {
        return new RetryableException(ErrorCode.NETWORK_ERROR, message, correlationId, 5, cause);
    }
}
