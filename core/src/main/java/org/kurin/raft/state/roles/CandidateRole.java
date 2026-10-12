package org.kurin.raft.state.roles;

import org.kurin.api.exception.KurinNoLeaderException;
import org.kurin.api.exception.KurinNotLeaderException;
import org.kurin.network.model.NodeAddress;
import org.kurin.raft.RaftRole;
import org.kurin.raft.rpc.AppendEntriesRequest;
import org.kurin.raft.rpc.AppendEntriesResponse;
import org.kurin.raft.rpc.ClientCommandRequest;
import org.kurin.raft.rpc.ClientQueryRequest;
import org.kurin.raft.rpc.RequestVoteRequest;
import org.kurin.raft.rpc.RequestVoteResponse;
import org.kurin.raft.state.NodeRole;
import org.kurin.raft.state.RaftState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class CandidateRole implements NodeRole {
    private static final Logger log = LoggerFactory.getLogger(CandidateRole.class);

    private int votesReceived;

    @Override
    public RaftRole role() {
        return RaftRole.CANDIDATE;
    }

    @Override
    public void onEnter(RaftState context) {
        context.beginElectionTerm();
        votesReceived = 1; // own vote

        long electionTerm = context.getCurrentTerm();
        log.info("Node {} started an election for term {} (quorum {})",
                context.getLocalAddress().asString(), electionTerm, context.quorumSize());

        if (votesReceived >= context.quorumSize()) {
            context.transitionTo(new LeaderRole());
            return;
        }
        context.resetElectionTimer();

        RequestVoteRequest request = new RequestVoteRequest(
                electionTerm,
                context.getLocalAddress(),
                context.getRaftLog().getLastLogIndex(),
                context.getRaftLog().getLastLogTerm()
        );

        for (NodeAddress peer : context.getPeers()) {
            context.getTransport().sendRequest(peer, request, context.getRpcTimeout()).whenComplete((response, error) ->
                    context.onRaftThread(() -> {
                        // Late answers of an election that is already over are ignored
                        if (context.getCurrentRole() != this || context.getCurrentTerm() != electionTerm) {
                            return;
                        }
                        if (error == null && response instanceof RequestVoteResponse voteResponse) {
                            processVoteResponse(context, voteResponse);
                        } else if (error != null) {
                            log.debug("No vote from {}: {}", peer.asString(), error.getMessage());
                        }
                    }));
        }
    }

    @Override
    public void onExit(RaftState context) {
        context.cancelElectionTimer();
    }

    @Override
    public void onElectionTimeout(RaftState context) {
        log.info("Election for term {} got no quorum on {}. Starting a new one.",
                context.getCurrentTerm(), context.getLocalAddress().asString());
        context.transitionTo(new CandidateRole());
    }

    private void processVoteResponse(RaftState context, RequestVoteResponse response) {
        if (response.term() > context.getCurrentTerm()) {
            context.stepDown(response.term(), null);
            return;
        }
        if (response.voteGranted()) {
            votesReceived++;
            if (votesReceived >= context.quorumSize()) {
                context.transitionTo(new LeaderRole());
            }
        }
    }

    @Override
    public RequestVoteResponse handleRequestVote(RaftState context, RequestVoteRequest request) {
        if (request.term() > context.getCurrentTerm()) {
            context.stepDown(request.term(), null);
            return context.getCurrentRole().handleRequestVote(context, request);
        }
        return new RequestVoteResponse(context.getCurrentTerm(), false);
    }

    @Override
    public AppendEntriesResponse handleAppendEntries(RaftState context, AppendEntriesRequest request) {
        if (request.term() >= context.getCurrentTerm()) {
            // A leader exists for this term (or a later one): the election is lost
            context.stepDown(request.term(), request.leaderId());
            return context.getCurrentRole().handleAppendEntries(context, request);
        }
        return AppendEntriesResponse.rejected(context.getCurrentTerm(), 0, 0);
    }

    @Override
    public CompletableFuture<Object> handleClientCommand(RaftState context, ClientCommandRequest request, boolean local) {
        return noLeader(context, local);
    }

    @Override
    public CompletableFuture<Object> handleClientQuery(RaftState context, ClientQueryRequest request, boolean local) {
        return noLeader(context, local);
    }

    private CompletableFuture<Object> noLeader(RaftState context, boolean local) {
        if (local) {
            return CompletableFuture.failedFuture(new KurinNoLeaderException("Leader election is in progress"));
        }
        return CompletableFuture.failedFuture(new KurinNotLeaderException(
                "Node " + context.getLocalAddress().asString() + " is not the leader", null));
    }
}
