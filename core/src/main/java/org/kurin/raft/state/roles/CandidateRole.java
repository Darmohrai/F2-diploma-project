package org.kurin.raft.state.roles;

import org.kurin.network.model.NodeAddress;
import org.kurin.raft.rpc.*;
import org.kurin.raft.state.NodeRole;
import org.kurin.raft.state.RaftState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class CandidateRole implements NodeRole {

    private static final Logger log = LoggerFactory.getLogger(CandidateRole.class);
    private int votesReceived;

    @Override
    public String roleName() {
        return "CANDIDATE";
    }

    @Override
    public void onEnter(RaftState context) {
        context.setCurrentTerm(context.getCurrentTerm() + 1);
        context.setVotedFor(context.getLocalAddress());
        context.flushMetadata();
        votesReceived = 1; // Vote for self

        int quorum = getQuorum(context);
        log.info("Started election for term {}. Quorum required: {}",
                context.getCurrentTerm(), quorum);

        if (votesReceived >= quorum) {
            log.info("Quorum reached immediately (single node mode)! Transitioning to LEADER.");
            context.transitionTo(new LeaderRole());
            return;
        }

        context.resetElectionTimer();
        RequestVoteRequest request = new RequestVoteRequest(
                context.getCurrentTerm(),
                context.getLocalAddress(),
                context.getRaftLog().getLastLogIndex(),
                context.getRaftLog().getLastLogTerm()
        );

        // Async broadcast to all peers
        for (NodeAddress peer : context.getPeers()) {
            context.getNetworkClient().sendRequest(peer, request).whenComplete((response, error) -> {
                context.getMailbox().submit(() -> {
                    // Ignore late responses if we changed role or term
                    if (context.getCurrentRole() != this || context.getCurrentTerm() != request.term()) {
                        return;
                    }
                    if (error == null && response instanceof RequestVoteResponse voteResponse) {
                        processVoteResponse(context, voteResponse);
                    } else if (error != null) {
                        log.warn("Failed to get vote from {}: {}", peer.asString(), error.getMessage());
                    }
                });
            });
        }
    }

    @Override
    public void onExit(RaftState context) {
        context.cancelElectionTimer();
    }

    private void processVoteResponse(RaftState context, RequestVoteResponse response) {
        if (response.term() > context.getCurrentTerm()) {
            log.info("Discovered higher term ({}). Reverting to FOLLOWER.", response.term());
            context.setCurrentTerm(response.term());
            context.transitionTo(new FollowerRole());
            return;
        }

        if (response.voteGranted()) {
            votesReceived++;
            log.info("Vote received. Total votes: {}/{}", votesReceived, getQuorum(context));

            if (votesReceived >= getQuorum(context)) {
                log.info("Quorum reached! Transitioning to LEADER.");
                context.transitionTo(new LeaderRole());
            }
        }
    }

    @Override
    public RequestVoteResponse handleRequestVote(RaftState context, RequestVoteRequest request) {
        if (request.term() > context.getCurrentTerm()) {
            context.setCurrentTerm(request.term());
            context.setVotedFor(null);
            context.transitionTo(new FollowerRole());
            context.flushMetadata();
            return context.getCurrentRole().handleRequestVote(context, request);
        }
        return new RequestVoteResponse(context.getCurrentTerm(), false);
    }

    @Override
    public AppendEntriesResponse handleAppendEntries(RaftState context, AppendEntriesRequest request) {
        if (request.term() >= context.getCurrentTerm()) {
            context.setCurrentTerm(request.term());
            context.transitionTo(new FollowerRole());
            return context.getCurrentRole().handleAppendEntries(context, request);
        }
        return new AppendEntriesResponse(context.getCurrentTerm(), false);
    }

    // Helper method to calculate the required majority
    private int getQuorum(RaftState context) {
        // Quorum = (Total Nodes / 2) + 1. Total nodes = peers + self (1)
        return ((context.getPeers().size() + 1) / 2) + 1;
    }

    @Override
    public CompletableFuture<Object> handleClientCommand(RaftState context, org.kurin.raft.rpc.ClientCommandRequest request) {
        String leaderHost = context.getVotedFor() != null ? context.getVotedFor().asString() : "UNKNOWN";
        return CompletableFuture.failedFuture(
                new IllegalStateException("I am not the leader. Redirect to: " + leaderHost)
        );
    }

    @Override
    public InstallSnapshotResponse handleInstallSnapshot(RaftState context, InstallSnapshotRequest request) {
        if (request.term() > context.getCurrentTerm()) {
            context.setCurrentTerm(request.term());
            context.transitionTo(new FollowerRole());
            return context.getCurrentRole().handleInstallSnapshot(context, request);
        }
        return new InstallSnapshotResponse(context.getCurrentTerm());
    }
}
