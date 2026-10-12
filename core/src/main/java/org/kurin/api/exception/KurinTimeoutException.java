package org.kurin.api.exception;

/**
 * No answer arrived in time. The outcome is unknown: a command may still have been committed and applied.
 * Retry with the same request id to get an idempotent result.
 */
public class KurinTimeoutException extends KurinException {
    public KurinTimeoutException(String message) {
        super(message);
    }
}
