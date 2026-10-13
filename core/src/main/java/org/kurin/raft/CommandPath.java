package org.kurin.raft;

import org.kurin.network.client.ConnectionManager.Reachability;
import org.kurin.network.model.NodeAddress;

import java.util.function.Function;

/**
 * Whether a command or a linearizable query submitted on this node can get to the leader right now.
 * <p>
 * This is narrower than knowing a leader. A follower learns who leads from what the leader sends it, over a
 * connection the leader opened; what the follower submits travels the other way, over a connection of its own.
 */
public enum CommandPath {
    /**
     * This node is the established leader, or it has an open connection to the leader it knows.
     */
    UP,
    /**
     * A leader is known and nothing is known to be wrong, but the path is not there yet: this node was just
     * elected and has not committed the first entry of its term, or it has no open connection to the leader.
     * A connection is opened by the first request that needs it, and one that was idle is closed again, so a
     * follower that forwards nothing is in this state most of the time. A request submitted now waits for the
     * connection.
     */
    CONNECTING,
    /**
     * Requests submitted now are not expected to succeed: the node is not started, its storage failed, it knows
     * no leader, or its last attempt to connect to the leader failed and the next one is not due yet.
     */
    DOWN;

    /**
     * @param started               whether the node is running
     * @param leadershipEstablished whether the node is the leader with a valid lease and a committed first entry
     * @param reachability          state of this node's own connection to a peer
     */
    static CommandPath of(boolean started, RaftStatus status, boolean leadershipEstablished,
                          Function<NodeAddress, Reachability> reachability) {
        if (!started || status.storageFailed() || status.leaderId() == null) {
            return DOWN;
        }
        if (status.leaderId().equals(status.nodeId())) {
            return leadershipEstablished ? UP : CONNECTING;
        }
        return switch (reachability.apply(NodeAddress.parse(status.leaderId()))) {
            case CONNECTED -> UP;
            case CONNECTING, NOT_CONNECTED -> CONNECTING;
            case PAUSED -> DOWN;
        };
    }
}
