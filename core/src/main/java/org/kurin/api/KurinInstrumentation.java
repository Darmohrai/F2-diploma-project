package org.kurin.api;

import java.util.Map;

/**
 * Tracing hook of the core: lets a tracing system see the operations of the node as spans and carry its
 * context from one node to another. The core knows no tracing library; the Spring starter implements this
 * with Micrometer Observation when that is present.
 * <p>
 * Span names used by the core: {@code kurin.command}, {@code kurin.query}, {@code kurin.command.forward},
 * {@code kurin.raft.apply}.
 */
public interface KurinInstrumentation {

    KurinInstrumentation NOOP = (name, tags, parentHeaders) -> Span.NOOP;

    /**
     * Starts a span.
     *
     * @param tags          low-cardinality attributes of the operation
     * @param parentHeaders trace context received from another node, to continue its trace; null to use
     *                      whatever context is current on the calling thread
     */
    Span start(String name, Map<String, String> tags, Map<String, String> parentHeaders);

    interface Span {
        Span NOOP = new Span() {
            @Override
            public Map<String, String> headers() {
                return Map.of();
            }

            @Override
            public void end(Throwable error) {
            }
        };

        /**
         * @return the context of this span in wire form (for example W3C {@code traceparent}), to be sent
         * along with a request so that the receiver can continue the trace. Never written to the Raft log.
         */
        Map<String, String> headers();

        /**
         * Adds an attribute that became known while the operation ran. Low cardinality only.
         */
        default void tag(String key, String value) {
        }

        /**
         * @param error the failure of the operation, or null if it succeeded
         */
        void end(Throwable error);
    }
}
