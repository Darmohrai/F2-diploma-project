package org.kurin.kurinlocksspringbootstarter;

import org.kurin.api.exception.KurinException;

/**
 * The key expression of a {@code @KurinDistributedLock} could not be evaluated to a non-null value.
 */
public class KurinLockKeyException extends KurinException {
    public KurinLockKeyException(String message) {
        super(message);
    }

    public KurinLockKeyException(String message, Throwable cause) {
        super(message, cause);
    }
}
