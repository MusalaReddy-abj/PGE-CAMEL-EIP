package com.pge.krakencis.exceptions;

public enum ErrorCode {

    // Validation errors (VAL-xxx)
    INVALID_INPUT("VAL-001", "Invalid input provided"),
    MISSING_REQUIRED_FIELD("VAL-002", "Required field is missing"),
    INVALID_FORMAT("VAL-003", "Field format is invalid"),
    INVALID_DATE_RANGE("VAL-004", "Date range is invalid"),

    // Integration errors (INT-xxx)
    ROUTE_PROCESSING_FAILED("INT-001", "Route processing failed"),
    MESSAGE_PROCESSING_FAILED("INT-002", "Message processing failed"),
    UNSUPPORTED_MESSAGE_TYPE("INT-003", "Message type is not supported"),

    // External service errors (EXT-xxx)
    EXTERNAL_SERVICE_UNAVAILABLE("EXT-001", "External service is unavailable"),
    EXTERNAL_SERVICE_TIMEOUT("EXT-002", "External service timed out"),
    EXTERNAL_SERVICE_ERROR("EXT-003", "External service returned an error"),
    EXTERNAL_SERVICE_AUTH_FAILED("EXT-004", "External service authentication failed"),

    // Transformation errors (TRF-xxx)
    TRANSFORMATION_FAILED("TRF-001", "Data transformation failed"),
    MAPPING_FAILED("TRF-002", "Field mapping failed"),
    SERIALIZATION_FAILED("TRF-003", "Serialization/deserialization failed"),

    // Retryable / transient errors (RET-xxx)
    TRANSIENT_ERROR("RET-001", "Transient error, retry eligible"),
    NETWORK_ERROR("RET-002", "Network connectivity error"),
    LOCK_ACQUISITION_FAILED("RET-003", "Could not acquire lock, retry eligible"),

    // System errors (SYS-xxx)
    INTERNAL_ERROR("SYS-001", "Internal system error"),
    DATABASE_ERROR("SYS-002", "Database operation failed"),
    CONFIGURATION_ERROR("SYS-003", "Application misconfiguration"),
    RESOURCE_NOT_FOUND("SYS-004", "Required resource not found");

    private final String code;
    private final String defaultMessage;

    ErrorCode(String code, String defaultMessage) {
        this.code = code;
        this.defaultMessage = defaultMessage;
    }

    public String getCode() {
        return code;
    }

    public String getDefaultMessage() {
        return defaultMessage;
    }

    @Override
    public String toString() {
        return code + " - " + defaultMessage;
    }
}
