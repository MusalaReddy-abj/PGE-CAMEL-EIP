package com.pge.krakencis.routes.rcdc.request;

import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.rcdc.request.RcdcTargetCallProcessor;
import com.pge.krakencis.processors.rcdc.request.RcdcTargetResponseProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.stereotype.Component;

@Component
public class RcdcRequestKafkaListner extends RouteBuilder {

    private static final StructuredLogger log = StructuredLogger.of(RcdcRequestKafkaListner.class);

    private final RcdcTargetCallProcessor     rcdcTargetCallProcessor;
    private final RcdcTargetResponseProcessor rcdcTargetResponseProcessor;

    public RcdcRequestKafkaListner(RcdcTargetCallProcessor     rcdcTargetCallProcessor,
                                   RcdcTargetResponseProcessor rcdcTargetResponseProcessor) {
        this.rcdcTargetCallProcessor     = rcdcTargetCallProcessor;
        this.rcdcTargetResponseProcessor = rcdcTargetResponseProcessor;
    }

    @Override
    public void configure() {
        final String uri =
            "kafka:{{kafka.topic.rcdc:kraken-rcdc-events}}"
            + "?brokers={{kafka.producer.brokers}}"
            + "&groupId={{kafka.consumer.group-id:kraken-cis-group}}"
            + "&autoOffsetReset={{kafka.consumer.auto-offset-reset:earliest}}"
            + "&maxPollRecords={{kafka.consumer.max-poll-records:500}}"
            + "&autoCommitEnable=false&allowManualCommit=true";

        onException(Exception.class).handled(true)
            .process(exchange -> {
                Exception ex = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                log.error("rcdcConsumerError",
                    exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class), ex,
                    "message", ex.getMessage());
            });

        from(uri).routeId("route-rcdc-kafka-consumer")
            .process(exchange -> {
                String cid = exchange.getIn().getHeader(KafkaConstants.KEY, String.class);
                exchange.setProperty(LogConstants.PROP_CORRELATION_ID, cid);
                log.info("rcdcMessageConsumed", cid,
                    "topic", exchange.getIn().getHeader(KafkaConstants.TOPIC),
                    "offset", exchange.getIn().getHeader(KafkaConstants.OFFSET));
            })
            .process(rcdcTargetCallProcessor)
            .process(rcdcTargetResponseProcessor);
    }
}
