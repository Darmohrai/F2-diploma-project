package org.kurin.raft.rpc;

public record InstallSnapshotResponse(
        long term
) {
}
