package org.kurin.raft.state;

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

    /**
     * The local log was flushed to disk: entries up to its durable index now survive a crash of this node.
     */
    default void onLogDurable(RaftState context) {
    }

    InstallSnapshotResponse handleInstallSnapshot(RaftState context, InstallSnapshotRequest request);

    /**
     * @return true if this node currently has reason to believe that a working leader exists: it heard from
     * one recently, or it is a leader in contact with a majority. Such a node refuses to help elect another.
     */
    boolean hasLiveLeader(RaftState context);

    /**
     * Answers a pre-vote probe. Never changes the state of this node.
     */
    default PreVoteResponse handlePreVote(RaftState context, PreVoteRequest request) {
        long lastLogTerm = context.getRaftLog().getLastLogTerm();
        long lastLogIndex = context.getRaftLog().getLastLogIndex();
        boolean logUpToDate = request.lastLogTerm() > lastLogTerm
                || (request.lastLogTerm() == lastLogTerm && request.lastLogIndex() >= lastLogIndex);
        boolean granted = request.term() > context.getCurrentTerm() && logUpToDate && !hasLiveLeader(context);
        return new PreVoteResponse(context.getCurrentTerm(), granted);
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
