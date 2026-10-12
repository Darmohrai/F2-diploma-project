package org.kurin.raft.rpc;

public sealed interface RaftRpc permits
        AppendEntriesRequest,
        RequestVoteRequest,
        PreVoteRequest,
        InstallSnapshotRequest,
        ClientCommandRequest,
        ClientQueryRequest {
}
