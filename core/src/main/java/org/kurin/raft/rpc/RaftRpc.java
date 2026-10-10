package org.kurin.raft.rpc;

public sealed interface RaftRpc permits
        AppendEntriesRequest,
        RequestVoteRequest,
        InstallSnapshotRequest,
        ClientCommandRequest,
        ClientQueryRequest {
}
