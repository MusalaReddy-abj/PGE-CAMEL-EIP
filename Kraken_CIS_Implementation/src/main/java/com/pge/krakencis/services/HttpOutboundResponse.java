package com.pge.krakencis.services;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class HttpOutboundResponse {

    private int     statusCode;
    private String  body;
    private boolean success;

    public boolean isClientError()  { return statusCode >= 400 && statusCode < 500; }
    public boolean isServerError()  { return statusCode >= 500; }
}
