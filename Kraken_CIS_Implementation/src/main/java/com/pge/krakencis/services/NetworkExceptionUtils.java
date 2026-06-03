package com.pge.krakencis.services;

import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

/**
 * Utility for classifying network-level exceptions as transient (retryable).
 *
 * <h3>Cause-chain traversal</h3>
 * HTTP client libraries (Spring's {@code RestClient}, Apache HttpClient) wrap
 * low-level network exceptions inside framework exceptions
 * (e.g. {@code ResourceAccessException} → {@code ConnectException}).
 * A single-level {@code getCause()} check misses these. This utility walks the
 * <em>full</em> cause chain so that connection-refused errors are correctly
 * identified as retryable regardless of how many wrapper layers surround the
 * original exception.
 *
 * <h3>Retryable conditions detected</h3>
 * <ul>
 *   <li>{@link ConnectException} — connection refused, target not reachable</li>
 *   <li>{@link SocketTimeoutException} — read or connect timeout</li>
 *   <li>{@link UnknownHostException} — DNS resolution failure</li>
 *   <li>{@link InterruptedIOException} — I/O interrupted (e.g. thread pool shutdown)</li>
 *   <li>Message fragments: "Connection refused", "timeout", "Temporary failure"</li>
 * </ul>
 */
public final class NetworkExceptionUtils {

    private NetworkExceptionUtils() {}

    /**
     * Returns {@code true} if any exception in the cause chain represents a
     * transient network failure worth retrying.
     *
     * <p>Walks the full {@code Throwable.getCause()} chain to handle multi-layer
     * wrapping (e.g. {@code ExternalServiceException} →
     * {@code ResourceAccessException} → {@code ConnectException}).
     */
    public static boolean isNetworkException(Exception e) {
        Throwable current = e;
        while (current != null) {
            if (isTransientCause(current)) {
                return true;
            }
            // Prevent infinite loop on circular cause chains (defensive)
            Throwable next = current.getCause();
            if (next == current) break;
            current = next;
        }
        return false;
    }

    // ─────────────────────────────────────────────────────────────────────────

    private static boolean isTransientCause(Throwable t) {
        if (t instanceof ConnectException
                || t instanceof SocketTimeoutException
                || t instanceof UnknownHostException
                || t instanceof InterruptedIOException) {
            return true;
        }
        String msg = t.getMessage();
        return msg != null && (
            msg.contains("Connection refused")
         || msg.contains("connect timed out")
         || msg.contains("Read timed out")
         || msg.contains("timeout")
         || msg.contains("Temporary failure")
         || msg.contains("No route to host")
         || msg.contains("Connection reset"));
    }
}
