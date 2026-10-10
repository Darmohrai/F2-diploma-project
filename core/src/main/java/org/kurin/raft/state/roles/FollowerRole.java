package org.kurin.raft.state.roles;

import org.kurin.network.model.NodeAddress;
import org.kurin.raft.log.LogEntry;
import org.kurin.raft.log.RaftLog;
import org.kurin.raft.rpc.*;
import org.kurin.raft.state.NodeRole;
import org.kurin.raft.state.RaftState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class FollowerRole implements NodeRole {

    private static final Logger log = LoggerFactory.getLogger(FollowerRole.class);

    @Override
    public String roleName() {
        return "FOLLOWER";
    }

    @Override
    public void onEnter(RaftState context) {
        log.info("Node has transitioned to the FOLLOWER state.");
        context.resetElectionTimer();
    }

    @Override
    public void onExit(RaftState context) {
        context.cancelElectionTimer();
    }

    @Override
    public RequestVoteResponse handleRequestVote(RaftState context, RequestVoteRequest request) {
        boolean stateChanged = false;

        if (request.term() < context.getCurrentTerm()) {
            return new RequestVoteResponse(context.getCurrentTerm(), false);
        }

        if (request.term() > context.getCurrentTerm()) {
            context.setCurrentTerm(request.term());
            context.setVotedFor(null);
            stateChanged = true;
        }

        boolean canVote = (context.getVotedFor() == null || context.getVotedFor().equals(request.candidateId()));
        long lastLogTerm = context.getRaftLog().getLastLogTerm();
        long lastLogIndex = context.getRaftLog().getLastLogIndex();

        boolean isLogUpToDate = (request.lastLogTerm() > lastLogTerm) ||
                (request.lastLogTerm() == lastLogTerm && request.lastLogIndex() >= lastLogIndex);

        boolean voteGranted = canVote && isLogUpToDate;

        if (voteGranted) {
            context.setVotedFor(request.candidateId());
            context.resetElectionTimer();
            stateChanged = true;
            log.info("Voted for candidate {} in term {}", request.candidateId().asString(), request.term());
        }

        if (stateChanged) {
            context.flushMetadata();
        }

        return new RequestVoteResponse(context.getCurrentTerm(), voteGranted);
    }

    @Override
    public AppendEntriesResponse handleAppendEntries(RaftState context, AppendEntriesRequest request) {
        if (request.term() < context.getCurrentTerm()) {
            return new AppendEntriesResponse(context.getCurrentTerm(), false);
        }
        if (request.term() > context.getCurrentTerm()) {
            context.setCurrentTerm(request.term());
            context.setVotedFor(null);
        }
        context.resetElectionTimer();
        context.setCurrentLeader(request.leaderId());

        RaftLog raftLog = context.getRaftLog();
        long prevLogIndex = request.prevLogIndex();

        if (prevLogIndex > 0) {
            if (prevLogIndex == raftLog.getBaseIndex()) {
                if (request.prevLogTerm() != raftLog.getBaseTerm()) {
                    return new AppendEntriesResponse(context.getCurrentTerm(), false);
                }
            } else if (prevLogIndex > raftLog.getBaseIndex()) {
                LogEntry prevEntry = raftLog.getEntry(prevLogIndex);
                if (prevEntry == null || prevEntry.term() != request.prevLogTerm()) {
                    log.warn("Log mismatch detected at index {}. Rejecting to force sync.", prevLogIndex);
                    return new AppendEntriesResponse(context.getCurrentTerm(), false);
                }
            }
        }

        long currentIndex = prevLogIndex + 1;
        for (LogEntry entry : request.entries()) {
            if (currentIndex <= raftLog.getBaseIndex()) {
                currentIndex++;
                continue;
            }

            LogEntry existingEntry = raftLog.getEntry(currentIndex);
            if (existingEntry != null && existingEntry.term() != entry.term()) {
                raftLog.truncateFrom(currentIndex);
                existingEntry = null;
            }
            if (existingEntry == null) {
                raftLog.append(entry);
            }
            currentIndex++;
        }

        if (request.leaderCommit() > context.getCommitIndex()) {
            long lastNewEntryIndex = prevLogIndex + request.entries().size();
            context.setCommitIndex(Math.min(request.leaderCommit(), lastNewEntryIndex));
            context.applyCommittedEntries();
        }

        return new AppendEntriesResponse(context.getCurrentTerm(), true);
    }

    @Override
    public CompletableFuture<Object> handleClientCommand(RaftState context, ClientCommandRequest request) {
        NodeAddress leader = context.getCurrentLeader();
        if (leader == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Cluster has no leader yet. Try again later."));
        }

        log.info("I am a FOLLOWER. Forwarding request {} to LEADER ({})", request.requestId(), leader.asString());
        return context.getNetworkClient().sendRequest(leader, request);
    }

    @Override
    public InstallSnapshotResponse handleInstallSnapshot(RaftState context, InstallSnapshotRequest request) {
        if (request.term() < context.getCurrentTerm()) {
            return new InstallSnapshotResponse(context.getCurrentTerm());
        }
        if (request.term() > context.getCurrentTerm()) {
            context.setCurrentTerm(request.term());
            context.setVotedFor(null);
        }
        context.resetElectionTimer();

        context.getStateMachine().installSnapshot(request.data());

        context.getRaftLog().compact(request.lastIncludedIndex(), request.lastIncludedTerm());

        context.setLastApplied(request.lastIncludedIndex());
        context.setCommitIndex(Math.max(context.getCommitIndex(), request.lastIncludedIndex()));

        context.flushMetadata();
        log.info("Successfully installed snapshot from leader up to index {}", request.lastIncludedIndex());

        return new InstallSnapshotResponse(context.getCurrentTerm());
    }

    @Override
    public CompletableFuture<Object> handleClientQuery(RaftState context, org.kurin.raft.rpc.ClientQueryRequest request) {
        NodeAddress leader = context.getCurrentLeader();
        if (leader == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Cluster has no leader yet. Try again later."));
        }
        log.debug("I am a FOLLOWER. Forwarding QUERY request {} to LEADER ({})", request.requestId(), leader.asString());
        return context.getNetworkClient().sendRequest(leader, request);
    }
}
