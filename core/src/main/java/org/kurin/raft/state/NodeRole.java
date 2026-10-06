package org.kurin.raft.state;

import org.kurin.raft.rpc.*;

import java.util.concurrent.CompletableFuture;

public interface NodeRole {

    default boolean isLeader() {
        return "LEADER".equals(roleName());
    }

    String roleName();

    void onEnter(RaftState context);

    void onExit(RaftState context);

    RequestVoteResponse handleRequestVote(RaftState context, RequestVoteRequest request);

    AppendEntriesResponse handleAppendEntries(RaftState context, AppendEntriesRequest request);

    CompletableFuture<Object> handleClientCommand(RaftState context, org.kurin.raft.rpc.ClientCommandRequest request);

    InstallSnapshotResponse handleInstallSnapshot(RaftState context, org.kurin.raft.rpc.InstallSnapshotRequest request);
}
