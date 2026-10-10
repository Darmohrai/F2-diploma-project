package org.kurin.raft;

import org.kurin.network.dispatcher.AsyncThreadPoolDispatcher;
import org.kurin.network.dto.ClusterMessage;
import org.kurin.raft.rpc.*;
import org.kurin.raft.state.NodeRole;
import org.kurin.raft.state.RaftState;

import java.util.concurrent.CompletableFuture;

public class RaftDispatcher extends AsyncThreadPoolDispatcher {

    private final RaftState raftState;

    public RaftDispatcher(RaftState raftState) {
        this.raftState = raftState;
    }

    @Override
    protected CompletableFuture<Object> processLogic(ClusterMessage message) {
        Object payload = message.getPayload();

        if (!(payload instanceof RaftRpc rpc)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown message type: " + payload.getClass().getSimpleName()));
        }

        NodeRole currentRole = raftState.getCurrentRole();

        return switch (rpc) {
            case AppendEntriesRequest req ->
                    CompletableFuture.completedFuture(currentRole.handleAppendEntries(raftState, req));
            case RequestVoteRequest req ->
                    CompletableFuture.completedFuture(currentRole.handleRequestVote(raftState, req));
            case InstallSnapshotRequest req ->
                    CompletableFuture.completedFuture(currentRole.handleInstallSnapshot(raftState, req));
            case ClientCommandRequest req -> currentRole.handleClientCommand(raftState, req);
            case ClientQueryRequest req -> currentRole.handleClientQuery(raftState, req);
        };
    }
}
