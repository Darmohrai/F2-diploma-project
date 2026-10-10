package org.kurin.raft.rpc;

import org.kurin.network.model.NodeAddress;

public record InstallSnapshotRequest(
        long term,
        NodeAddress leaderId,
        long lastIncludedIndex,
        long lastIncludedTerm,
        byte[] data
) implements RaftRpc {
}
