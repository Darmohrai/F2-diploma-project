package org.kurin.api.exception;

/**
 * The contacted node is not the leader. {@link #leaderHint()} holds the node id ({@code host:port})
 * of the leader known to that node, or null if it does not know one.
 */
public class KurinNotLeaderException extends KurinException {
    private final String leaderHint;

    public KurinNotLeaderException(String message, String leaderHint) {
        super(message);
        this.leaderHint = leaderHint;
    }

    public String leaderHint() {
        return leaderHint;
    }
}
