package com.pge.krakencis.exceptions;

public class TransformationException extends KrakenBaseException {

    public TransformationException(ErrorCode errorCode, String message, String correlationId) {
        super(errorCode, ErrorCategory.TRANSFORMATION, message, correlationId);
    }

    public TransformationException(ErrorCode errorCode, String message,
                                    String correlationId, Throwable cause) {
        super(errorCode, ErrorCategory.TRANSFORMATION, message, correlationId, cause);
    }

    public static TransformationException mappingFailed(String sourceField, String targetField,
                                                         String correlationId, Throwable cause) {
        return (TransformationException) new TransformationException(
            ErrorCode.MAPPING_FAILED,
            String.format("Failed to map '%s' -> '%s'", sourceField, targetField),
            correlationId, cause)
            .withContext("sourceField", sourceField)
            .withContext("targetField", targetField);
    }

    public static TransformationException serializationFailed(String type,
                                                               String correlationId,
                                                               Throwable cause) {
        return (TransformationException) new TransformationException(
            ErrorCode.SERIALIZATION_FAILED,
            "Serialization failed for type: " + type,
            correlationId, cause)
            .withContext("type", type);
    }
}
