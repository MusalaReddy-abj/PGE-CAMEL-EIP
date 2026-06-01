package com.pge.krakencis.exceptions;

public class IntegrationException extends KrakenBaseException {

    public IntegrationException(ErrorCode errorCode, String message, String correlationId) {
        super(errorCode, ErrorCategory.INTEGRATION, message, correlationId);
    }

    public IntegrationException(ErrorCode errorCode, String message,
                                 String correlationId, Throwable cause) {
        super(errorCode, ErrorCategory.INTEGRATION, message, correlationId, cause);
    }

    public static IntegrationException routeFailed(String routeId, String correlationId,
                                                    Throwable cause) {
        return (IntegrationException) new IntegrationException(
            ErrorCode.ROUTE_PROCESSING_FAILED,
            "Route processing failed: " + routeId,
            correlationId, cause)
            .withContext("routeId", routeId);
    }

    public static IntegrationException messageProcessingFailed(String messageType,
                                                                String correlationId,
                                                                Throwable cause) {
        return (IntegrationException) new IntegrationException(
            ErrorCode.MESSAGE_PROCESSING_FAILED,
            "Message processing failed for type: " + messageType,
            correlationId, cause)
            .withContext("messageType", messageType);
    }
}
