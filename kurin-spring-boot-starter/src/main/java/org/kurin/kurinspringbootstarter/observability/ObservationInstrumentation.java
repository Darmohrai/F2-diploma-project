package org.kurin.kurinspringbootstarter.observability;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.transport.ReceiverContext;
import io.micrometer.observation.transport.SenderContext;
import org.kurin.api.KurinInstrumentation;

import java.util.HashMap;
import java.util.Map;

/**
 * Reports the spans of the core as Micrometer observations. With a tracing bridge on the classpath
 * (Micrometer Tracing with Brave or OpenTelemetry) every observation becomes a span, and the propagating
 * handlers of that bridge write and read the trace context ({@code traceparent} and friends) through the
 * sender and receiver contexts used here. The starter itself depends on the Observation API only.
 */
public class ObservationInstrumentation implements KurinInstrumentation {
    private final ObservationRegistry registry;

    public ObservationInstrumentation(ObservationRegistry registry) {
        this.registry = registry;
    }

    @Override
    public Span start(String name, Map<String, String> tags, Map<String, String> parentHeaders) {
        Observation observation;
        Map<String, String> outgoing = new HashMap<>();

        if (parentHeaders != null && !parentHeaders.isEmpty()) {
            // Continues the trace that arrived with the request
            ReceiverContext<Map<String, String>> received = new ReceiverContext<>(Map::get);
            received.setCarrier(parentHeaders);
            observation = Observation.createNotStarted(name, () -> received, registry);
        } else {
            // A new span of the trace current on this thread; its context is written into the carrier at start
            SenderContext<Map<String, String>> sent = new SenderContext<>((carrier, key, value) -> carrier.put(key, value));
            sent.setCarrier(outgoing);
            observation = Observation.createNotStarted(name, () -> sent, registry);
        }
        tags.forEach(observation::lowCardinalityKeyValue);
        observation.start();

        return new Span() {
            @Override
            public Map<String, String> headers() {
                if (outgoing.isEmpty() && parentHeaders != null && !parentHeaders.isEmpty()) {
                    // A received span that is sent on: a child with a sender context writes the headers for the next hop
                    SenderContext<Map<String, String>> sent = new SenderContext<>((carrier, key, value) -> carrier.put(key, value));
                    sent.setCarrier(outgoing);
                    Observation.createNotStarted(name + ".send", () -> sent, registry).parentObservation(observation).start().stop();
                }
                return outgoing;
            }

            @Override
            public void tag(String key, String value) {
                observation.lowCardinalityKeyValue(key, value);
            }

            @Override
            public void end(Throwable error) {
                if (error != null) {
                    observation.error(error);
                }
                observation.stop();
            }
        };
    }
}
