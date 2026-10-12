package org.kurin.api.exception;

/**
 * The node has not been started yet or is already shut down.
 */
public class KurinNotStartedException extends KurinException {
    public KurinNotStartedException(String message) {
        super(message);
    }
}
