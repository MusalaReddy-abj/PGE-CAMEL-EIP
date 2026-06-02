package com.pge.krakencis.services;

import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

/**
 * Utility for classifying network-level exceptions as transient (retryable).
 *
 * <p>Extracted from {@code RcdcTargetCallProcessor} and
 * {@code RcdcMdmNotificationProcessor} to eliminate duplication.
 */
public final class NetworkExceptionUtils {

    private NetworkExceptionUtils() {}

    /**
     * Returns {@code true} if the exception represents a transient network
     * failure that is worth retrying: connection refused, timeout, unknown host,
     * interrupted I/O, or well-known error message fragments.
     */
    public static boolean isNetworkException(Exception e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        return cause instanceof ConnectException
            || cause instanceof SocketTimeoutException
            || cause instanceof UnknownHostException
            || cause instanceof InterruptedIOException
            || (e.getMessage() != null && (
                e.getMessage().contains("Connection refused")
             || e.getMessage().contains("timeout")
             || e.getMessage().contains("Temporary failure")));
    }
}
