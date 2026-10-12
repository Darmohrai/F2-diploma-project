package org.kurin.api.exception;

/**
 * Base class of all exceptions reported by Kurin to the application.
 */
public class KurinException extends RuntimeException {
    public KurinException(String message) {
        super(message);
    }

    public KurinException(String message, Throwable cause) {
        super(message, cause);
    }
}
