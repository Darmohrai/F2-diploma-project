package org.kurin.raft.rpc;

import org.kurin.network.model.NodeAddress;

/**
 * One chunk of a serialized snapshot, sent by the leader to a follower whose log is too far behind.
 *
 * @param lastIncludedIndex index of the last entry covered by the snapshot; identifies the transfer
 * @param offset            position of {@code data} within the serialized snapshot
 * @param done              true for the last chunk
 */
public record InstallSnapshotRequest(
        long term,
        NodeAddress leaderId,
        long lastIncludedIndex,
        long lastIncludedTerm,
        int offset,
        byte[] data,
        boolean done
) implements RaftRpc {
}
