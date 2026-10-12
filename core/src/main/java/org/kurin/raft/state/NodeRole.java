package org.kurin.raft.state;

import org.kurin.raft.RaftRole;
import org.kurin.raft.rpc.AppendEntriesRequest;
import org.kurin.raft.rpc.AppendEntriesResponse;
import org.kurin.raft.rpc.ClientCommandRequest;
import org.kurin.raft.rpc.ClientQueryRequest;
import org.kurin.raft.rpc.RequestVoteRequest;
import org.kurin.raft.rpc.RequestVoteResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Behaviour of a node in one Raft role. Every method runs on the Raft thread.
 */
public interface NodeRole {

    RaftRole role();

    void onEnter(RaftState context);

    void onExit(RaftState context);

    /**
     * The election timer of this role instance fired.
     */
    void onElectionTimeout(RaftState context);

    /**
     * The heartbeat timer of this role instance fired. Only the leader runs one.
     */
    default void onHeartbeatTick(RaftState context) {
    }

    RequestVoteResponse handleRequestVote(RaftState context, RequestVoteRequest request);

    AppendEntriesResponse handleAppendEntries(RaftState context, AppendEntriesRequest request);

    /**
     * @param local true if the command was submitted on this node, false if another node forwarded it.
     *              A forwarded command is never forwarded again.
     */
    CompletableFuture<Object> handleClientCommand(RaftState context, ClientCommandRequest request, boolean local);

    CompletableFuture<Object> handleClientQuery(RaftState context, ClientQueryRequest request, boolean local);
}
