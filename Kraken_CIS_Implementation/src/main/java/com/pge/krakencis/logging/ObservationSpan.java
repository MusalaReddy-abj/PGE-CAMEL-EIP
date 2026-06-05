package com.pge.krakencis.logging;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

/**
 * Wraps a Camel {@link Processor} in a named Micrometer Observation so it shows up as a
 * child span in the trace.
 *
 * <p>Camel Observation auto-instruments endpoints ({@code from}/{@code to}), but NOT
 * arbitrary {@code .process(bean)} steps — so heavy work done inside a bean processor
 * (e.g. CSV parsing, archiving) produces no span and the route looks empty. Wrapping such
 * a step with {@link #wrap} creates a child observation under the current (route) context,
 * so it nests correctly into the same connected trace and is exported like any other span.
 *
 * <p>Usage in a route:
 * <pre>{@code .process(observationSpan.wrap("profileReads.parseCsv", csvProcessor)) }</pre>
 */
@Component
public class ObservationSpan {

    private final ObservationRegistry registry;

    public ObservationSpan(ObservationRegistry registry) {
        this.registry = registry;
    }

    /** Returns a processor that runs {@code delegate} inside a child span named {@code name}. */
    public Processor wrap(String name, Processor delegate) {
        return exchange -> {
            Observation observation = Observation.createNotStarted(name, registry).start();
            try (Observation.Scope scope = observation.openScope()) {
                delegate.process(exchange);
            } catch (Exception e) {
                observation.error(e);
                throw e;
            } finally {
                observation.stop();
            }
        };
    }
}
