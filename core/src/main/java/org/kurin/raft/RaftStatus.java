package org.kurin.raft;

/**
 * Immutable snapshot of a node's Raft state, safe to read from any thread. A new instance is published
 * after every change of role, term, leader or applied index.
 *
 * @param leaderId             node id of the known leader, null if unknown
 * @param leaseValidUntilNanos {@link System#nanoTime()} value until which this node may act as leader without
 *                             contacting a quorum; 0 if the node holds no lease
 */
public record RaftStatus(
        RaftRole role,
        long term,
        String leaderId,
        long commitIndex,
        long lastApplied,
        long lastLogIndex,
        long leaseValidUntilNanos,
        String nodeId
) {
    public boolean isLeader() {
        return role == RaftRole.LEADER;
    }

    public boolean hasValidLease() {
        return role == RaftRole.LEADER && leaseValidUntilNanos != 0 && leaseValidUntilNanos - System.nanoTime() > 0;
    }
}
