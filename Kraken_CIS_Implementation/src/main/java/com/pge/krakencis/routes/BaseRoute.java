package com.pge.krakencis.routes;

import com.pge.krakencis.exceptions.KrakenBaseException;
import com.pge.krakencis.exceptions.RetryableException;
import com.pge.krakencis.exceptions.TransformationException;
import com.pge.krakencis.exceptions.ValidationException;
import com.pge.krakencis.logging.RouteLoggingProcessor;
import com.pge.krakencis.processors.CorrelationIdProcessor;
import com.pge.krakencis.processors.RouteExceptionProcessor;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.RouteDefinition;

import java.util.function.Consumer;

/**
 * Base class for all Camel routes.
 *
 * Provides processingRoute() — a template that pre-wires every cross-cutting
 * concern so concrete routes declare ONLY their business-specific steps:
 *
 *   processingRoute("direct:my-route", "route-id", "operationName", route ->
 *       route
 *           .process(myProcessor)
 *           .to("direct:next-step")
 *           .process(myResponseProcessor)
 *   );
 *
 * Cross-cutting concerns handled automatically:
 *   ✓ ValidationException      → 400 immediately (no retry — payload fault)
 *   ✓ TransformationException  → 422 immediately (no retry — payload fault)
 *   ✓ RetryableException       → retry 3× (1 s / 2 s / 4 s exp backoff) → 503
 *   ✓ KrakenBaseException      → 500 immediately (domain error)
 *   ✓ Exception                → retry 3× (1 s / 2 s / 4 s exp backoff) → 503
 *   ✓ Correlation ID seed/propagation
 *   ✓ Route entry / exit audit logging
 *
 * NOTE: RetryableException must be declared BEFORE KrakenBaseException because
 * RetryableException extends KrakenBaseException. Without this explicit handler,
 * Camel would match KrakenBaseException first and return HTTP 500 with no retry.
 */
public abstract class BaseRoute extends RouteBuilder {

    protected final CorrelationIdProcessor  correlationIdProcessor;
    protected final RouteLoggingProcessor   routeLoggingProcessor;
    protected final RouteExceptionProcessor exceptionProcessor;

    protected BaseRoute(CorrelationIdProcessor  correlationIdProcessor,
                        RouteLoggingProcessor   routeLoggingProcessor,
                        RouteExceptionProcessor exceptionProcessor) {
        this.correlationIdProcessor = correlationIdProcessor;
        this.routeLoggingProcessor  = routeLoggingProcessor;
        this.exceptionProcessor     = exceptionProcessor;
    }

    protected void processingRoute(String fromUri, String routeId, String operation,
                                    Consumer<RouteDefinition> businessSteps) {

        RouteDefinition route = from(fromUri).routeId(routeId);

        route.onException(ValidationException.class)
            .handled(true).process(exceptionProcessor.validation(operation)).end();

        route.onException(TransformationException.class)
            .handled(true).process(exceptionProcessor.transformation(operation)).end();

        // Transient failures from downstream services (e.g. ODR mock down, Kafka unavailable).
        // Must be declared BEFORE KrakenBaseException — RetryableException is a subclass and
        // would otherwise be swallowed by the KrakenBaseException handler (HTTP 500, no retry).
        route.onException(RetryableException.class)
            .maximumRedeliveries(3)
            .redeliveryDelay(1_000)
            .backOffMultiplier(2)
            .useExponentialBackOff()
            .retryAttemptedLogLevel(LoggingLevel.WARN)
            .handled(true)
            .process(exceptionProcessor.systemWithRetryInfo(operation))
            .end();

        route.onException(KrakenBaseException.class)
            .handled(true).process(exceptionProcessor.domain(operation)).end();

        // Catch-all for unexpected transient failures — same retry behaviour as RetryableException.
        route.onException(Exception.class)
            .maximumRedeliveries(3)
            .redeliveryDelay(1_000)
            .backOffMultiplier(2)
            .useExponentialBackOff()
            .retryAttemptedLogLevel(LoggingLevel.WARN)
            .handled(true)
            .process(exceptionProcessor.systemWithRetryInfo(operation))
            .end();

        route.process(correlationIdProcessor)
             .process(routeLoggingProcessor.entry(operation));

        businessSteps.accept(route);

        route.process(routeLoggingProcessor.exit(operation));
    }
}
