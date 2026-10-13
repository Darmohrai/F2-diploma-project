package org.kurin.kurinlocks.exception;

import org.kurin.api.exception.KurinException;

/**
 * The lock is busy: another owner holds it, and still held it when the wait ended. Also thrown when the waiting
 * thread is interrupted.
 * <p>
 * It is not thrown when the cluster could not answer: then nothing is known about the lock, and the caller gets
 * the exception that says why ({@code KurinNoLeaderException}, {@code KurinRemoteException},
 * {@code KurinTimeoutException}).
 */
public class KurinLockAcquisitionException extends KurinException {
    public KurinLockAcquisitionException(String message) {
        super(message);
    }
}
