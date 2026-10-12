package org.kurin.api.exception;

/**
 * A message could not be serialized or deserialized (unregistered type, payload over the size limit).
 */
public class KurinSerializationException extends KurinException {
    public KurinSerializationException(String message) {
        super(message);
    }

    public KurinSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
