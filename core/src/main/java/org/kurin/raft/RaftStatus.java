package org.kurin.raft;

/**
 * Immutable snapshot of a node's Raft state, safe to read from any thread. A new instance is published
 * after every change of role, term, leader or applied index.
 *
 * @param leaderId             node id of the known leader, null if unknown
 * @param leaseValidUntilNanos {@link System#nanoTime()} value until which this node may act as leader without
 *                             contacting a quorum; 0 if the node holds no lease
 * @param storageFailure       why the node cannot write its durable state, null while storage works. A node with
 *                             failed storage reports the role FOLLOWER but neither votes nor accepts entries,
 *                             commands or queries until its storage works again.
 */
public record RaftStatus(
        RaftRole role,
        long term,
        String leaderId,
        long commitIndex,
        long lastApplied,
        long lastLogIndex,
        long leaseValidUntilNanos,
        String nodeId,
        String storageFailure
) {
    public RaftStatus(RaftRole role, long term, String leaderId, long commitIndex, long lastApplied, long lastLogIndex,
                      long leaseValidUntilNanos, String nodeId) {
        this(role, term, leaderId, commitIndex, lastApplied, lastLogIndex, leaseValidUntilNanos, nodeId, null);
    }

    public boolean isLeader() {
        return role == RaftRole.LEADER;
    }

    public boolean hasValidLease() {
        return role == RaftRole.LEADER && leaseValidUntilNanos != 0 && leaseValidUntilNanos - System.nanoTime() > 0;
    }

    /**
     * @return true while the node is out of the cluster because it cannot write its durable state
     */
    public boolean storageFailed() {
        return storageFailure != null;
    }
}
