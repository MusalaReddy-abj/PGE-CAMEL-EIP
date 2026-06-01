package com.pge.krakencis.routes.rcdc.response;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.rcdc.response.RcdcMdmNotificationProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.stereotype.Component;

@Component
public class RcdcResponseHesKafkaListner extends RouteBuilder {

    private static final StructuredLogger log = StructuredLogger.of(RcdcResponseHesKafkaListner.class);

    private final RcdcMdmNotificationProcessor rcdcMdmNotificationProcessor;

    public RcdcResponseHesKafkaListner(RcdcMdmNotificationProcessor rcdcMdmNotificationProcessor) {
        this.rcdcMdmNotificationProcessor = rcdcMdmNotificationProcessor;
    }

    @Override
    public void configure() {
        final String uri =
            "kafka:{{kafka.topic.rcdc-hes-response:kraken-rcdc-hes-response-events}}"
            + "?brokers={{kafka.producer.brokers}}"
            + "&groupId={{kafka.consumer.group-id:kraken-cis-group}}-mdm"
            + "&autoOffsetReset={{kafka.consumer.auto-offset-reset:earliest}}"
            + "&maxPollRecords={{kafka.consumer.max-poll-records:500}}"
            + "&autoCommitEnable=false&allowManualCommit=true";

        onException(Exception.class).handled(true)
            .process(exchange -> {
                Exception ex = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                log.error("rcdcHesConsumerError",
                    exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class), ex,
                    "message", ex.getMessage());
            });

        from(uri).routeId("route-rcdc-hes-kafka-consumer")
            .process(exchange -> {
                String cid = exchange.getIn().getHeader(KafkaConstants.KEY, String.class);
                exchange.setProperty(LogConstants.PROP_CORRELATION_ID, cid);
                log.info("rcdcHesMessageConsumed", cid,
                    "topic",  exchange.getIn().getHeader(KafkaConstants.TOPIC),
                    "offset", exchange.getIn().getHeader(KafkaConstants.OFFSET));
            })
            .process(rcdcMdmNotificationProcessor)
            .log("MDM notified [correlationId=${exchangeProperty."
                + LogConstants.PROP_CORRELATION_ID + "}]"
                + " [status=${header." + Exchange.HTTP_RESPONSE_CODE + "}]");
    }
}
