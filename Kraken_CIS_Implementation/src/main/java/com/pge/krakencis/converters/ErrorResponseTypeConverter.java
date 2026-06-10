package com.pge.krakencis.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pge.krakencis.models.ErrorResponse;
import org.apache.camel.Converter;
import org.apache.camel.TypeConverters;
import org.springframework.stereotype.Component;

/**
 * Camel type converter: {@link ErrorResponse} → JSON {@code String}.
 *
 * <p>Error responses are now set on the exchange as an {@code ErrorResponse} <b>object</b>
 * (see {@link com.pge.krakencis.processors.RouteExceptionProcessor}). On JSON-binding routes
 * ({@code /alarms}, {@code /rcdc/response}) the REST binding marshals that object to a JSON
 * object. On {@code bindingMode=off} routes ({@code /rcdc}, {@code /odr}) there is no REST
 * data format, so this converter turns the object into a JSON string for sending.
 *
 * <p>Either way the client receives a proper JSON <b>object</b>. Previously the error body
 * was a pre-serialized JSON <b>string</b>, which the JSON binding then double-encoded into a
 * quoted, escaped string ({@code "{\"status\":400,...}"}).
 *
 * <p>Registered automatically: camel-spring-boot adds every {@link TypeConverters} bean to
 * the type-converter registry.
 */
@Component
public class ErrorResponseTypeConverter implements TypeConverters {

    private final ObjectMapper objectMapper;

    public ErrorResponseTypeConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Converter
    public String toJson(ErrorResponse errorResponse) throws Exception {
        return objectMapper.writeValueAsString(errorResponse);
    }
}
