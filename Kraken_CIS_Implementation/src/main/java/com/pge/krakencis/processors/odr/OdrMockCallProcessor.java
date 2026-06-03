package com.pge.krakencis.processors.odr;

import com.pge.krakencis.exceptions.ExternalServiceException;
import com.pge.krakencis.exceptions.RetryableException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.BaseProcessor;
import com.pge.krakencis.services.NetworkExceptionUtils;
import com.pge.krakencis.services.SOAOdrMockService;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

@Component
public class OdrMockCallProcessor extends BaseProcessor {

    private static final StructuredLogger log          = StructuredLogger.of(OdrMockCallProcessor.class);
    private static final String           SERVICE_NAME = "SOA-ODR-MockService";

    private final SOAOdrMockService soaOdrMockService;

    public OdrMockCallProcessor(SOAOdrMockService soaOdrMockService) {
        this.soaOdrMockService = soaOdrMockService;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
        String rawXml        = exchange.getProperty("odr.rawXml", String.class);
        String mRID          = exchange.getProperty("odr.mRID", String.class);

        log.info("odrMockCallStarted", correlationId, "mRID", mRID);

        try {
            String responseXml = soaOdrMockService.sendRequest(rawXml, correlationId);

            exchange.getIn().setBody(responseXml);
            exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE, 200);
            exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "text/xml; charset=utf-8");
            exchange.getIn().setHeader("X-Correlation-ID", correlationId);

            log.info("odrMockCallCompleted", correlationId, "mRID", mRID);

        } catch (RetryableException | ExternalServiceException e) {
            // Re-throw typed exceptions as-is.
            // RetryableException must not be caught by BaseRoute.onException(KrakenBaseException)
            // which would return HTTP 500 immediately with no retry.
            // Letting it propagate as-is ensures BaseRoute.onException(Exception) retries 3×
            // and then returns HTTP 503 to the caller.
            log.warn("odrMockCallFailed", correlationId,
                "mRID", mRID, "error", e.getMessage(),
                "exceptionType", e.getClass().getSimpleName());
            throw e;
        } catch (Exception e) {
            if (NetworkExceptionUtils.isNetworkException(e)) {
                Throwable networkCause = NetworkExceptionUtils.rootNetworkCause(e);
                log.warn("odrMockNetworkError", correlationId,
                    "mRID", mRID, "error", e.getMessage());
                throw RetryableException.networkError(
                    "Network error calling " + SERVICE_NAME, correlationId, networkCause);
            }
            throw e;
        }
    }
}
