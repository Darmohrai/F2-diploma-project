package org.kurin.kurinlocks.exception;

import org.kurin.api.exception.KurinException;

/**
 * The lock could not be obtained: another owner holds it, or the cluster did not answer in time.
 */
public class KurinLockAcquisitionException extends KurinException {
    public KurinLockAcquisitionException(String message) {
        super(message);
    }
}
