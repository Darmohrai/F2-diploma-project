package org.kurin.routing;

/**
 * The primary of a shard is down, and the operation cannot be served by a replica: it is a write, or a read
 * that must not be stale. Raised at once, without waiting for a connection timeout, so that the application
 * can answer "temporarily unavailable" while eventual reads keep working.
 */
public class KurinPrimaryUnavailableException extends KurinRoutingException {
    private final String shard;

    public KurinPrimaryUnavailableException(String shard) {
        super("The primary database of shard '" + shard + "' is unavailable");
        this.shard = shard;
    }

    public String shard() {
        return shard;
    }
}
