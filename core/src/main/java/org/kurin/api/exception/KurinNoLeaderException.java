package org.kurin.api.exception;

/**
 * The cluster currently has no known leader (election in progress or quorum lost). Nothing was submitted.
 */
public class KurinNoLeaderException extends KurinException {
    public KurinNoLeaderException(String message) {
        super(message);
    }
}
