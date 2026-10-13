package org.kurin.api.exception;

/**
 * The node cannot write its durable state (term and vote, log, snapshots) and has taken itself out of the cluster
 * until the storage works again. A command that fails with this exception was not accepted by this node; if it
 * was already in flight, its outcome is unknown and it can be retried with the same request id.
 */
public class KurinStorageException extends KurinException {
    public KurinStorageException(String message) {
        super(message);
    }

    public KurinStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
