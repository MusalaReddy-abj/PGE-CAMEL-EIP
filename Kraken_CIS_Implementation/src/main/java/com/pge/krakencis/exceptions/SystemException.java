package com.pge.krakencis.exceptions;

public class SystemException extends KrakenBaseException {

    public SystemException(ErrorCode errorCode, String message, String correlationId) {
        super(errorCode, ErrorCategory.SYSTEM, message, correlationId);
    }

    public SystemException(ErrorCode errorCode, String message,
                            String correlationId, Throwable cause) {
        super(errorCode, ErrorCategory.SYSTEM, message, correlationId, cause);
    }

    public static SystemException databaseError(String operation, String correlationId,
                                                 Throwable cause) {
        return (SystemException) new SystemException(
            ErrorCode.DATABASE_ERROR,
            "Database operation failed: " + operation,
            correlationId, cause)
            .withContext("operation", operation);
    }

    public static SystemException configurationError(String property, String correlationId) {
        return (SystemException) new SystemException(
            ErrorCode.CONFIGURATION_ERROR,
            "Missing or invalid configuration: " + property,
            correlationId)
            .withContext("property", property);
    }

    public static SystemException internalError(String message, String correlationId,
                                                 Throwable cause) {
        return new SystemException(ErrorCode.INTERNAL_ERROR, message, correlationId, cause);
    }
}
