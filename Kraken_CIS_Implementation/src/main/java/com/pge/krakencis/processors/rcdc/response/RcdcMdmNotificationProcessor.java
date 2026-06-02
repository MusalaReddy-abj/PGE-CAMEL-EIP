package com.pge.krakencis.processors.rcdc.response;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pge.krakencis.exceptions.ErrorCode;
import com.pge.krakencis.exceptions.ExternalServiceException;
import com.pge.krakencis.exceptions.RetryableException;
import com.pge.krakencis.exceptions.TransformationException;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.mappers.rcdc.response.RcdcMdmNotificationMapper;
import com.pge.krakencis.models.rcdc.response.RcdcHesResponseMessage;
import com.pge.krakencis.processors.BaseProcessor;
import com.pge.krakencis.services.SOAMDMNotificationService;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

/**
 * Sends an RCDC switch-state notification to the MDM SOAP service.
 *
 * <p>Retry logic is handled at the Camel route level
 * ({@link com.pge.krakencis.routes.rcdc.response.RcdcResponseHesKafkaListner}).
 * This processor throws typed exceptions so the route can decide:
 * <ul>
 *   <li>{@link TransformationException} — JSON/SOAP mapping failed → DLQ immediately</li>
 *   <li>{@link RetryableException}      — 5xx / network error → retry 3× then retry queue</li>
 *   <li>{@link ExternalServiceException}— 4xx client error → DLQ immediately</li>
 * </ul>
 */
@Component
public class RcdcMdmNotificationProcessor extends BaseProcessor {

    private static final StructuredLogger log          = StructuredLogger.of(RcdcMdmNotificationProcessor.class);
    private static final String           SERVICE_NAME = "SOA-MDM-Service";

    private final RcdcMdmNotificationMapper rcdcMdmNotificationMapper;
    private final SOAMDMNotificationService soaMdmNotificationService;
    private final ObjectMapper              objectMapper;

    public RcdcMdmNotificationProcessor(RcdcMdmNotificationMapper rcdcMdmNotificationMapper,
                                         SOAMDMNotificationService soaMdmNotificationService,
                                         ObjectMapper              objectMapper) {
        this.rcdcMdmNotificationMapper = rcdcMdmNotificationMapper;
        this.soaMdmNotificationService  = soaMdmNotificationService;
        this.objectMapper               = objectMapper;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String json = exchange.getIn().getBody(String.class);

        RcdcHesResponseMessage response;
        try {
            response = objectMapper.readValue(json, RcdcHesResponseMessage.class);
        } catch (Exception e) {
            String cid = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
            throw new TransformationException(
                ErrorCode.SERIALIZATION_FAILED,
                "Failed to deserialise MDM notification payload: " + e.getMessage(), cid);
        }

        String correlationId = response.getHeader().getCorrelationID();
        exchange.setProperty(LogConstants.PROP_CORRELATION_ID, correlationId);

        log.info("rcdcMdmNotificationProcessing", correlationId,
            "mRID",      response.getPayload().getDefaultResponse().getEndDeviceAsset().getMRID(),
            "replyCode", response.getReply().getReplyCode());

        String soapXml;
        try {
            soapXml = rcdcMdmNotificationMapper.toSoapXml(response, correlationId);
        } catch (Exception e) {
            throw new TransformationException(
                ErrorCode.MAPPING_FAILED,
                "Failed to build MDM SOAP envelope: " + e.getMessage(), correlationId);
        }

        callMdmService(soapXml, correlationId);
    }

    private void callMdmService(String soapXml, String correlationId) throws Exception {
        try {
            int status = soaMdmNotificationService.sendNotification(soapXml, correlationId);

            if (status >= 200 && status < 300) {
                log.info("mdmNotificationSent", correlationId, "httpStatus", status);
                return;
            }

            log.warn("mdmNotificationFailed", correlationId, "httpStatus", status);

            // Retryable: 5xx, 408 (timeout), 429 (rate-limit), 404 (service not found — may be transient)
            if (status >= 500 || status == 408 || status == 429 || status == 404) {
                throw RetryableException.transient_(
                    SERVICE_NAME + " returned HTTP " + status, correlationId);
            }
            // All other 4xx → permanent client error, goes to DLQ
            throw ExternalServiceException.httpError(SERVICE_NAME, status, "", correlationId);

        } catch (RetryableException | ExternalServiceException | TransformationException e) {
            throw e;
        } catch (Exception e) {
            if (isNetworkException(e)) {
                log.warn("mdmNotificationNetworkError", correlationId,
                    "error", e.getMessage(), "exceptionType", e.getClass().getSimpleName());
                throw RetryableException.networkError(
                    "Network error calling " + SERVICE_NAME, correlationId, e);
            }
            throw e;
        }
    }

    private boolean isNetworkException(Exception e) {
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
