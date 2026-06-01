package com.pge.krakencis.models.rcdc.request;

public enum RcdcState {
    CONNECT, DISCONNECT;

    public static RcdcState from(String value) {
        if (value == null) throw new IllegalArgumentException("state is required");
        try {
            return valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Invalid state '" + value.trim() + "'. Allowed: CONNECT, DISCONNECT");
        }
    }
}
