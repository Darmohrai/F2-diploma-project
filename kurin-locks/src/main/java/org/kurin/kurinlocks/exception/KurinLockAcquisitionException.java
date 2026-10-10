package org.kurin.kurinlocks.exception;

public class KurinLockAcquisitionException extends RuntimeException {
    public KurinLockAcquisitionException(String message) {
        super(message);
    }
}
