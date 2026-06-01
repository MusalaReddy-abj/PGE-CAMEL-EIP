package com.pge.krakencis.routes;

import com.pge.krakencis.configs.KafkaProducerConfig;
import com.pge.krakencis.logging.LogConstants;
import com.pge.krakencis.logging.StructuredLogger;
import com.pge.krakencis.processors.KafkaPublishProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.stereotype.Component;

import java.util.Collection;

/**
 * Generic Kafka publishing route.
 *
 * Caller sets exchange properties before calling direct:publishToKafka:
 *   - LogConstants.KAFKA_TOPIC  (String, required) — target topic
 *   - LogConstants.KAFKA_KEY    (String, optional) — message key
 *
 * Body: any object or Collection<?>.
 * Returns: Integer published count.
 */
@Component
public class KafkaRoute extends RouteBuilder {

    private static final StructuredLogger log = StructuredLogger.of(KafkaRoute.class);

    private final KafkaProducerConfig   kafkaProducerConfig;
    private final KafkaPublishProcessor kafkaPublishProcessor;

    public KafkaRoute(KafkaProducerConfig kafkaProducerConfig,
                      KafkaPublishProcessor kafkaPublishProcessor) {
        this.kafkaProducerConfig   = kafkaProducerConfig;
        this.kafkaPublishProcessor = kafkaPublishProcessor;
    }

    @Override
    public void configure() {

        final String kafkaEndpoint = "kafka:${exchangeProperty." + LogConstants.KAFKA_TOPIC + "}?"
            + kafkaProducerConfig.buildQueryString();

        from("direct:publishToKafka")
            .routeId("route-publish-to-kafka")

            .onException(Exception.class)
                .handled(true)
                .process(exchange -> {
                    String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
                    String topic = exchange.getProperty(LogConstants.KAFKA_TOPIC, String.class);
                    Exception ex = exchange.getException();
                    log.error("kafkaPublishFailed", correlationId, ex,
                        "topic", topic,
                        "error", ex.getMessage());
                })
                .setBody(constant(0))
            .end()

            .process(exchange -> {
                String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
                String topic = exchange.getProperty(LogConstants.KAFKA_TOPIC, String.class);
                Object body = exchange.getIn().getBody();
                int messageCount = (body instanceof Collection) ? ((Collection<?>) body).size() : 1;
                
                log.debug("kafkaPublishStarted", correlationId,
                    "topic", topic,
                    "messageCount", messageCount,
                    "bodyType", body != null ? body.getClass().getSimpleName() : "null");
            })

            .choice()

                .when(body().isInstanceOf(Collection.class))
                    .setProperty("publishedCount", simple("${body.size()}"))
                    .split(body())
                        .process(kafkaPublishProcessor.prepare())
                        .marshal().json(JsonLibrary.Jackson)
                        .toD(kafkaEndpoint)
                        .process(kafkaPublishProcessor.audit())
                    .end()
                .endChoice()

                .otherwise()
                    .setProperty("publishedCount", constant(1))
                    .process(kafkaPublishProcessor.prepare())
                    .marshal().json(JsonLibrary.Jackson)
                    .toD(kafkaEndpoint)
                    .process(kafkaPublishProcessor.audit())

            .end()

            .process(exchange -> {
                String correlationId = exchange.getProperty(LogConstants.PROP_CORRELATION_ID, String.class);
                String topic = exchange.getProperty(LogConstants.KAFKA_TOPIC, String.class);
                Integer publishedCount = exchange.getProperty("publishedCount", Integer.class);
                
                log.info("kafkaPublishCompleted", correlationId,
                    "topic", topic,
                    "publishedCount", publishedCount != null ? publishedCount : 0);
            })

            .setBody(exchangeProperty("publishedCount"));
    }
}
