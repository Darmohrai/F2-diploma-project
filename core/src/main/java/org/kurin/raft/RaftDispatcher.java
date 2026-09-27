package org.kurin.raft;

import org.kurin.network.dispatcher.AsyncThreadPoolDispatcher;
import org.kurin.network.dto.ClusterMessage;
import org.kurin.raft.rpc.*;
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

        if (payload instanceof AppendEntriesRequest req) {
            return CompletableFuture.completedFuture(raftState.getCurrentRole().handleAppendEntries(raftState, req));
        } else if (payload instanceof RequestVoteRequest req) {
            return CompletableFuture.completedFuture(raftState.getCurrentRole().handleRequestVote(raftState, req));
        } else if (payload instanceof InstallSnapshotRequest req) {
            return CompletableFuture.completedFuture(raftState.getCurrentRole().handleInstallSnapshot(raftState, req));
        } else if (payload instanceof ClientCommandRequest req) {
            return raftState.getCurrentRole().handleClientCommand(raftState, req);
        }

        return CompletableFuture.failedFuture(
                new IllegalArgumentException("Unknown Raft message type: " + payload.getClass().getSimpleName())
        );
    }
}
