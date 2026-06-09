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
import com.pge.krakencis.services.NetworkExceptionUtils;
import com.pge.krakencis.services.SOAMDMNotificationService;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

/**
 * Sends an RCDC switch-state notification to the MDM SOAP service.
 *
 * <p>Throws typed exceptions so the Camel route ({@link com.pge.krakencis.routes.rcdc.response.RcdcResponseHesKafkaListner})
 * can decide routing:
 * <ul>
 *   <li>{@link TransformationException} — JSON/SOAP mapping failed → DLQ</li>
 *   <li>{@link RetryableException}      — 5xx / 404 / network → retry 3× → retry queue</li>
 *   <li>{@link ExternalServiceException}— other 4xx → DLQ</li>
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

            // SOAP Server faults are mapped to 500 by SOAMDMNotificationService
            if (status >= 500 || status == 408 || status == 429 || status == 404) {
                throw RetryableException.transient_(
                    SERVICE_NAME + " returned HTTP " + status, correlationId);
            }
            throw ExternalServiceException.httpError(SERVICE_NAME, status, "", correlationId);

        } catch (RetryableException | ExternalServiceException | TransformationException e) {
            // Re-throw typed exceptions as-is — no re-wrapping that could pollute the cause chain
            throw e;
        } catch (Exception e) {
            if (NetworkExceptionUtils.isNetworkException(e)) {
                // Pass only the root network cause (ConnectException, SocketTimeoutException, etc.)
                // NOT the full exception e — it may wrap ExternalServiceException which would cause
                // Camel's onException(ExternalServiceException) to match and route to DLQ instead
                // of the retry topic.
                Throwable networkCause = NetworkExceptionUtils.rootNetworkCause(e);
                log.warn("mdmNotificationNetworkError", correlationId,
                    "error", e.getMessage(), "exceptionType", e.getClass().getSimpleName());
                throw RetryableException.networkError(
                    "Network error calling " + SERVICE_NAME, correlationId, networkCause);
            }
            throw e;
        }
    }
}
