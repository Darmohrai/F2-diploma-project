package org.kurin.raft.state.roles;

import org.kurin.api.exception.KurinStorageException;
import org.kurin.raft.RaftRole;
import org.kurin.raft.rpc.AppendEntriesRequest;
import org.kurin.raft.rpc.AppendEntriesResponse;
import org.kurin.raft.rpc.ClientCommandRequest;
import org.kurin.raft.rpc.ClientQueryRequest;
import org.kurin.raft.rpc.InstallSnapshotRequest;
import org.kurin.raft.rpc.InstallSnapshotResponse;
import org.kurin.raft.rpc.PreVoteRequest;
import org.kurin.raft.rpc.PreVoteResponse;
import org.kurin.raft.rpc.RequestVoteRequest;
import org.kurin.raft.rpc.RequestVoteResponse;
import org.kurin.raft.state.NodeRole;
import org.kurin.raft.state.RaftState;

import java.util.concurrent.CompletableFuture;

/**
 * The node cannot write its durable state. Everything a Raft node does relies on remembering it: a vote that is
 * forgotten can be given twice, an acknowledged entry that is lost breaks a commit. So the node does nothing
 * until the storage works again: it does not vote, does not stand for election, accepts no entries and answers
 * no clients. The rest of the cluster sees it as a node that is down. {@link RaftState} retries the storage and
 * brings the node back as a follower.
 */
public class StorageFailedRole implements NodeRole {

    @Override
    public RaftRole role() {
        return RaftRole.FOLLOWER;
    }

    @Override
    public void onEnter(RaftState context) {
        context.cancelElectionTimer();
        context.cancelHeartbeatTimer();
    }

    @Override
    public void onExit(RaftState context) {
        // Nothing was started in this role
    }

    @Override
    public void onElectionTimeout(RaftState context) {
        // No election timer runs in this role; a callback queued before the failure has nothing to do
    }

    @Override
    public boolean hasLiveLeader(RaftState context) {
        return false;
    }

    @Override
    public PreVoteResponse handlePreVote(RaftState context, PreVoteRequest request) {
        throw context.storageUnavailable();
    }

    @Override
    public RequestVoteResponse handleRequestVote(RaftState context, RequestVoteRequest request) {
        throw context.storageUnavailable();
    }

    @Override
    public AppendEntriesResponse handleAppendEntries(RaftState context, AppendEntriesRequest request) {
        throw context.storageUnavailable();
    }

    @Override
    public InstallSnapshotResponse handleInstallSnapshot(RaftState context, InstallSnapshotRequest request) {
        throw context.storageUnavailable();
    }

    @Override
    public CompletableFuture<Object> handleClientCommand(RaftState context, ClientCommandRequest request, boolean local) {
        KurinStorageException failure = context.storageUnavailable();
        return CompletableFuture.failedFuture(failure);
    }

    @Override
    public CompletableFuture<Object> handleClientQuery(RaftState context, ClientQueryRequest request, boolean local) {
        KurinStorageException failure = context.storageUnavailable();
        return CompletableFuture.failedFuture(failure);
    }
}
