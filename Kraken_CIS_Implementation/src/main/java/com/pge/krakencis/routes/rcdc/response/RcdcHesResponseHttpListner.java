package com.pge.krakencis.routes.rcdc.response;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.models.rcdc.response.RcdcHesResponseMessage;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import com.pge.krakencis.processors.rcdc.response.RcdcHesAckProcessor;
import com.pge.krakencis.processors.rcdc.response.RcdcHesResponseProcessor;
import com.pge.krakencis.routes.BaseRoute;
import org.apache.camel.model.rest.RestParamType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * HTTP listener that receives RCDC response callbacks from the HES (Head-End System)
 * and publishes them to a Kafka topic for downstream processing.
 *
 * <p>Exposes {@code POST /api/v1/rcdc/response}. The body must be a valid
 * {@link com.pge.krakencis.models.rcdc.response.RcdcHesResponseMessage} JSON document.
 * An optional {@code X-Correlation-ID} request header is accepted; one is generated
 * automatically if absent.
 *
 * <h3>Processing steps</h3>
 * <ol>
 *   <li>{@link com.pge.krakencis.processors.rcdc.response.RcdcHesResponseProcessor}
 *       — validates the message and extracts the correlation ID and mRID.</li>
 *   <li>Publishes the validated payload to
 *       {@code kafka.topic.rcdc-hes-response} (default:
 *       {@code kraken-rcdc-hes-response-events}).</li>
 *   <li>{@link com.pge.krakencis.processors.rcdc.response.RcdcHesAckProcessor}
 *       — builds an HTTP 202 acknowledgement response.</li>
 * </ol>
 *
 * <h3>Route ID</h3>
 * {@code route-post-rcdc-hes-response}
 */
@Component
public class RcdcHesResponseHttpListner extends BaseRoute {

    private static final String OPERATION = "postRCDCHESResponse";

    private final RcdcHesResponseProcessor rcdcHesResponseProcessor;
    private final RcdcHesAckProcessor      rcdcHesAckProcessor;

    @Value("${kafka.topic.rcdc-hes-response:kraken-rcdc-hes-response-events}")
    private String rcdcHesResponseTopic;

    public RcdcHesResponseHttpListner(CorrelationIdProcessor   correlationIdProcessor,
                                       RouteLoggingProcessor    routeLoggingProcessor,
                                       RouteExceptionProcessor  exceptionProcessor,
                                       RcdcHesResponseProcessor rcdcHesResponseProcessor,
                                       RcdcHesAckProcessor      rcdcHesAckProcessor) {
        super(correlationIdProcessor, routeLoggingProcessor, exceptionProcessor);
        this.rcdcHesResponseProcessor = rcdcHesResponseProcessor;
        this.rcdcHesAckProcessor      = rcdcHesAckProcessor;
    }

    @Override
    public void configure() {
        rest("/v1/rcdc/response")
            .post()
                .description("Receive RCDC response from HES and publish to Kafka")
                .consumes("application/json")
                .produces("application/json")
                .param()
                    .name("X-Correlation-ID").type(RestParamType.header)
                    .required(false)
                .endParam()
                .type(RcdcHesResponseMessage.class)
                .to("direct:process-rcdc-hes-response");

        processingRoute("direct:process-rcdc-hes-response",
                        "route-post-rcdc-hes-response", OPERATION, route ->
            route
                .process(rcdcHesResponseProcessor)
                .setProperty(LogConstants.KAFKA_TOPIC, constant(rcdcHesResponseTopic))
                .to("direct:publishToKafka")
                .process(rcdcHesAckProcessor)
        );
    }
}
