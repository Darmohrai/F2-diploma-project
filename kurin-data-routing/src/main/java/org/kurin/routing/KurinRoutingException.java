package org.kurin.routing;

/**
 * Base class of the errors raised by Kurin data routing.
 */
public class KurinRoutingException extends RuntimeException {
    public KurinRoutingException(String message) {
        super(message);
    }

    public KurinRoutingException(String message, Throwable cause) {
        super(message, cause);
    }
}
