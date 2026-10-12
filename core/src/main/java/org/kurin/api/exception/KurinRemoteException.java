package org.kurin.api.exception;

/**
 * Communication with another node failed (connection refused or closed, handshake rejected, remote error).
 */
public class KurinRemoteException extends KurinException {
    public KurinRemoteException(String message) {
        super(message);
    }

    public KurinRemoteException(String message, Throwable cause) {
        super(message, cause);
    }
}
