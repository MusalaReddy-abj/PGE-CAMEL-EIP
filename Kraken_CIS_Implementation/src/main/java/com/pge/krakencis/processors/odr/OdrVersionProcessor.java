package com.pge.krakencis.processors.odr;

import com.pge.krakencis.exceptions.ErrorCode;
import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.BaseProcessor;
import io.opentelemetry.api.trace.Span;
import org.apache.camel.Exchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Resolves and ENFORCES the requested ODR <b>minor</b> version on the v1 major endpoint.
 *
 * <p>Minor versions are negotiated via the {@code X-API-Version} request header — NOT the
 * URL. The major version lives in the path ({@code /api/v1/odr}); minor versions
 * ({@code 1.0}, {@code 1.1}, ...) are backward-compatible and selected per request:
 * <ul>
 *   <li>header absent → {@code odr.version.default}</li>
 *   <li>header not in {@code odr.version.supported} → 400 (ValidationException)</li>
 *   <li>header below {@code odr.version.minimum} → 400 (forces clients off retired minors)</li>
 * </ul>
 * The resolved version is stored as {@code odr.resolvedVersion} (so the response enricher
 * can add the right fields), echoed back as the {@code X-API-Version} response header, and
 * stamped on the span as {@code api.version}.
 */
@Component
public class OdrVersionProcessor extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(OdrVersionProcessor.class);

    public static final String HEADER                = "X-API-Version";
    public static final String PROP_RESOLVED_VERSION = "odr.resolvedVersion";

    /** Comma-separated supported minor versions of the v1 major, e.g. {@code 1.0,1.1}. */
    @Value("${odr.version.supported:1.0,1.1}") private List<String> supported;
    @Value("${odr.version.minimum:1.0}")       private String minimum;
    @Value("${odr.version.default:1.1}")       private String defaultVersion;

    @Override
    public void process(Exchange exchange) {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String requested     = exchange.getIn().getHeader(HEADER, String.class);

        String resolved = (requested == null || requested.isBlank())
            ? defaultVersion
            : requested.trim();

        if (!supported.contains(resolved)) {
            throw new ValidationException(ErrorCode.INVALID_INPUT,
                "Unsupported " + HEADER + " '" + resolved + "'. Supported: " + supported,
                correlationId);
        }
        if (compare(resolved, minimum) < 0) {
            throw new ValidationException(ErrorCode.INVALID_INPUT,
                HEADER + " '" + resolved + "' is below the minimum supported version " + minimum,
                correlationId);
        }

        exchange.setProperty(PROP_RESOLVED_VERSION, resolved);
        exchange.getIn().setHeader(HEADER, resolved);     // echo the negotiated version back to the client

        Span span = Span.current();
        if (span.getSpanContext().isValid()) {
            span.setAttribute("api.version", resolved);
        }

        log.info("odrVersionResolved", correlationId,
            "requested", requested != null ? requested : "(default)",
            "resolved",  resolved);
    }

    /** Compares dotted numeric versions: {@code compare("1.0","1.1") < 0}. */
    public static int compare(String a, String b) {
        String[] pa = a.split("\\.");
        String[] pb = b.split("\\.");
        int n = Math.max(pa.length, pb.length);
        for (int i = 0; i < n; i++) {
            int x = i < pa.length ? toInt(pa[i]) : 0;
            int y = i < pb.length ? toInt(pb[i]) : 0;
            if (x != y) return Integer.compare(x, y);
        }
        return 0;
    }

    private static int toInt(String s) {
        try { return Integer.parseInt(s.trim()); } catch (NumberFormatException e) { return 0; }
    }
}
