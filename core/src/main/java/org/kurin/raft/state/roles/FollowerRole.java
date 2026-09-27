package org.kurin.raft.state.roles;

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

        RaftLog raftLog = context.getRaftLog();
        if (request.prevLogIndex() > 0) {
            LogEntry prevEntry = raftLog.getEntry(request.prevLogIndex());
            if (prevEntry == null || prevEntry.term() != request.prevLogTerm()) {
                log.warn("Log mismatch detected. Rejecting AppendEntries to force synchronization.");
                return new AppendEntriesResponse(context.getCurrentTerm(), false);
            }
        }

        long currentIndex = request.prevLogIndex() + 1;
        for (LogEntry entry : request.entries()) {
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
            long lastNewEntryIndex = request.prevLogIndex() + request.entries().size();
            context.setCommitIndex(Math.min(request.leaderCommit(), lastNewEntryIndex));
            log.debug("CommitIndex updated to: {}", context.getCommitIndex());

            context.applyCommittedEntries();
        }

        return new AppendEntriesResponse(context.getCurrentTerm(), true);
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
}
