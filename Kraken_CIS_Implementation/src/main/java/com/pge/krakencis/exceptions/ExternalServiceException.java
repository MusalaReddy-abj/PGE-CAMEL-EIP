package com.pge.krakencis.exceptions;

public class ExternalServiceException extends KrakenBaseException {

    private final String serviceName;
    private final Integer httpStatusCode;

    public ExternalServiceException(ErrorCode errorCode, String serviceName,
                                     String message, String correlationId) {
        super(errorCode, ErrorCategory.EXTERNAL_SERVICE, message, correlationId);
        this.serviceName    = serviceName;
        this.httpStatusCode = null;
        withContext("serviceName", serviceName);
    }

    public ExternalServiceException(ErrorCode errorCode, String serviceName,
                                     String message, String correlationId,
                                     Integer httpStatusCode, Throwable cause) {
        super(errorCode, ErrorCategory.EXTERNAL_SERVICE, message, correlationId, cause);
        this.serviceName    = serviceName;
        this.httpStatusCode = httpStatusCode;
        withContext("serviceName", serviceName);
        if (httpStatusCode != null) {
            withContext("httpStatus", httpStatusCode);
        }
    }

    public String getServiceName() {
        return serviceName;
    }

    public Integer getHttpStatusCode() {
        return httpStatusCode;
    }

    public static ExternalServiceException unavailable(String serviceName,
                                                        String correlationId, Throwable cause) {
        return new ExternalServiceException(
            ErrorCode.EXTERNAL_SERVICE_UNAVAILABLE,
            serviceName,
            serviceName + " is unavailable",
            correlationId, null, cause);
    }

    public static ExternalServiceException timeout(String serviceName, String correlationId) {
        return new ExternalServiceException(
            ErrorCode.EXTERNAL_SERVICE_TIMEOUT,
            serviceName,
            serviceName + " call timed out",
            correlationId, null, null);
    }

    public static ExternalServiceException httpError(String serviceName, int statusCode,
                                                      String body, String correlationId) {
        return (ExternalServiceException) new ExternalServiceException(
            ErrorCode.EXTERNAL_SERVICE_ERROR,
            serviceName,
            String.format("%s returned HTTP %d", serviceName, statusCode),
            correlationId, statusCode, null)
            .withContext("responseBody", body);
    }
}
