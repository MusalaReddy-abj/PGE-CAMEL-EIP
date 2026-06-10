package com.pge.krakencis.processors.odr;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.BaseProcessor;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Applies the v1 <b>minor</b>-version response differences (additive only).
 *
 * <p>Runs after {@link OdrMockCallProcessor} (body = mock response XML) and before the SOAP
 * wrap, based on the version resolved by {@link OdrVersionProcessor}:
 * <ul>
 *   <li><b>1.0</b> — base response (unchanged).</li>
 *   <li><b>1.1</b> — base response + additive {@code <processedAt>} field.</li>
 * </ul>
 * Each minor version is purely additive over the previous one, so a 1.0 client is never
 * broken by 1.1 being available.
 */
@Component
public class OdrV1ResponseEnricher extends BaseProcessor {

    private static final StructuredLogger log = StructuredLogger.of(OdrV1ResponseEnricher.class);

    @Override
    public void process(Exchange exchange) {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String resolved      = exchange.getProperty(OdrVersionProcessor.PROP_RESOLVED_VERSION, String.class);
        if (resolved == null) {
            return;
        }

        // ── 1.1+ : additive <processedAt> field (absent in 1.0) ───────────────────
        if (OdrVersionProcessor.compare(resolved, "1.1") >= 0) {
            String body = exchange.getIn().getBody(String.class);
            if (body == null) {
                body = "";
            }
            exchange.getIn().setBody(body + "\n<processedAt>" + Instant.now() + "</processedAt>");
            log.info("odrV1MinorEnriched", correlationId, "resolvedVersion", resolved, "added", "processedAt");
        }
    }
}
