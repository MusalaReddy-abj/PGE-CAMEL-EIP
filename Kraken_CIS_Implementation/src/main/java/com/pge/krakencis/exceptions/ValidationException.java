package com.pge.krakencis.exceptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ValidationException extends KrakenBaseException {

    private final List<String> violations;

    public ValidationException(ErrorCode errorCode, String message, String correlationId) {
        super(errorCode, ErrorCategory.VALIDATION, message, correlationId);
        this.violations = new ArrayList<>();
    }

    public ValidationException(ErrorCode errorCode, String message,
                                String correlationId, List<String> violations) {
        super(errorCode, ErrorCategory.VALIDATION, message, correlationId);
        this.violations = new ArrayList<>(violations);
    }

    public List<String> getViolations() {
        return Collections.unmodifiableList(violations);
    }

    public static ValidationException missingField(String fieldName, String correlationId) {
        ValidationException ex = new ValidationException(
            ErrorCode.MISSING_REQUIRED_FIELD,
            "Required field is missing: " + fieldName,
            correlationId);
        ex.withContext("field", fieldName);
        return ex;
    }

    public static ValidationException invalidFormat(String fieldName, String expectedFormat,
                                                     String correlationId) {
        ValidationException ex = new ValidationException(
            ErrorCode.INVALID_FORMAT,
            String.format("Field '%s' does not match expected format: %s", fieldName, expectedFormat),
            correlationId);
        ex.withContext("field",          fieldName);
        ex.withContext("expectedFormat", expectedFormat);
        return ex;
    }

    public static ValidationException multipleViolations(List<String> violations,
                                                          String correlationId) {
        return new ValidationException(
            ErrorCode.INVALID_INPUT,
            "Validation failed with " + violations.size() + " violation(s)",
            correlationId,
            violations);
    }
}
