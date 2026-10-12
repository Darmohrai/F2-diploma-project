package org.kurin.raft;

import org.kurin.api.exception.KurinNotStartedException;
import org.kurin.api.exception.KurinRemoteException;
import org.kurin.network.dispatcher.MessageDispatcher;
import org.kurin.network.dto.ClusterMessage;
import org.kurin.raft.rpc.AppendEntriesRequest;
import org.kurin.raft.rpc.ClientCommandRequest;
import org.kurin.raft.rpc.ClientQueryRequest;
import org.kurin.raft.rpc.RaftRpc;
import org.kurin.raft.rpc.RequestVoteRequest;
import org.kurin.raft.state.NodeRole;
import org.kurin.raft.state.RaftState;

import java.util.concurrent.CompletableFuture;

/**
 * Moves every inbound RPC from the network threads onto the Raft thread.
 */
public class RaftDispatcher implements MessageDispatcher {

    private final RaftState raftState;

    public RaftDispatcher(RaftState raftState) {
        this.raftState = raftState;
    }

    @Override
    public CompletableFuture<Object> dispatchAsync(ClusterMessage message) {
        if (!(message.getPayload() instanceof RaftRpc rpc)) {
            Object payload = message.getPayload();
            return CompletableFuture.failedFuture(new KurinRemoteException(
                    "Unsupported request payload: " + (payload != null ? payload.getClass().getName() : "null")));
        }

        CompletableFuture<Object> result = new CompletableFuture<>();
        try {
            raftState.execute(() -> {
                try {
                    handle(rpc).whenComplete((value, error) -> {
                        if (error != null) {
                            result.completeExceptionally(error);
                        } else {
                            result.complete(value);
                        }
                    });
                } catch (RuntimeException e) {
                    result.completeExceptionally(e);
                }
            });
        } catch (KurinNotStartedException e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    private CompletableFuture<Object> handle(RaftRpc rpc) {
        NodeRole role = raftState.getCurrentRole();
        if (role == null) {
            return CompletableFuture.failedFuture(new KurinNotStartedException("Node is still starting"));
        }
        return switch (rpc) {
            case AppendEntriesRequest request -> CompletableFuture.completedFuture(role.handleAppendEntries(raftState, request));
            case RequestVoteRequest request -> CompletableFuture.completedFuture(role.handleRequestVote(raftState, request));
            case org.kurin.raft.rpc.PreVoteRequest request -> CompletableFuture.completedFuture(role.handlePreVote(raftState, request));
            case org.kurin.raft.rpc.InstallSnapshotRequest request -> CompletableFuture.completedFuture(role.handleInstallSnapshot(raftState, request));
            case ClientCommandRequest request -> role.handleClientCommand(raftState, request, false);
            case ClientQueryRequest request -> role.handleClientQuery(raftState, request, false);
        };
    }
}
