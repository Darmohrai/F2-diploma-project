package org.kurin.raft.state.roles;

import org.kurin.api.exception.KurinNoLeaderException;
import org.kurin.api.exception.KurinNotLeaderException;
import org.kurin.network.dto.RpcResult;
import org.kurin.network.model.NodeAddress;
import org.kurin.raft.RaftRole;
import org.kurin.raft.log.LogEntry;
import org.kurin.raft.log.RaftLog;
import org.kurin.raft.rpc.AppendEntriesRequest;
import org.kurin.raft.rpc.AppendEntriesResponse;
import org.kurin.raft.rpc.ClientCommandRequest;
import org.kurin.raft.rpc.ClientQueryRequest;
import org.kurin.api.ReadConsistency;
import org.kurin.raft.rpc.InstallSnapshotRequest;
import org.kurin.raft.rpc.InstallSnapshotResponse;
import org.kurin.raft.snapshot.Snapshot;
import java.io.ByteArrayOutputStream;
import org.kurin.raft.rpc.PreVoteRequest;
import org.kurin.raft.rpc.PreVoteResponse;
import org.kurin.raft.rpc.RequestVoteRequest;
import org.kurin.raft.rpc.RequestVoteResponse;
import org.kurin.raft.state.NodeRole;
import org.kurin.raft.state.RaftState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class FollowerRole implements NodeRole {

    private static final Logger log = LoggerFactory.getLogger(FollowerRole.class);

    // Identifies the running pre-vote round; answers of an abandoned round are ignored
    private long preVoteRound;
    private int preVotesGranted;

    @Override
    public RaftRole role() {
        return RaftRole.FOLLOWER;
    }

    @Override
    public void onEnter(RaftState context) {
        context.resetElectionTimer();
    }

    @Override
    public void onExit(RaftState context) {
        context.cancelElectionTimer();
    }

    @Override
    public boolean hasLiveLeader(RaftState context) {
        return context.heardFromLeaderRecently();
    }

    /**
     * The leader went silent. Before disturbing the cluster with a new term, ask whether a majority would
     * vote for this node at all; only then become a candidate.
     */
    @Override
    public void onElectionTimeout(RaftState context) {
        long round = ++preVoteRound;
        preVotesGranted = 1; // own vote
        context.setCurrentLeader(null);
        context.resetElectionTimer();

        if (preVotesGranted >= context.quorumSize()) {
            context.transitionTo(new CandidateRole());
            return;
        }

        long currentTerm = context.getCurrentTerm();
        log.debug("No heartbeat from a leader on {}. Probing for an election in term {}.",
                context.getLocalAddress().asString(), currentTerm + 1);
        PreVoteRequest request = new PreVoteRequest(currentTerm + 1, context.getLocalAddress(),
                context.getRaftLog().getLastLogIndex(), context.getRaftLog().getLastLogTerm());

        for (NodeAddress peer : context.getPeers()) {
            context.getTransport().sendRequest(peer, request, context.getRpcTimeout()).whenComplete((response, error) ->
                    context.onRaftThread(() -> {
                        if (context.getCurrentRole() != this || round != preVoteRound || context.getCurrentTerm() != currentTerm) {
                            return;
                        }
                        if (error != null || !(response instanceof PreVoteResponse preVote)) {
                            return;
                        }
                        if (preVote.term() > context.getCurrentTerm()) {
                            context.stepDown(preVote.term(), null);
                            return;
                        }
                        if (preVote.voteGranted() && ++preVotesGranted >= context.quorumSize()) {
                            log.info("A majority would vote for {}. Starting an election.", context.getLocalAddress().asString());
                            context.transitionTo(new CandidateRole());
                        }
                    }));
        }
    }

    @Override
    public RequestVoteResponse handleRequestVote(RaftState context, RequestVoteRequest request) {
        if (request.term() < context.getCurrentTerm()) {
            return new RequestVoteResponse(context.getCurrentTerm(), false);
        }
        if (context.heardFromLeaderRecently()) {
            // Leader stickiness: a working leader is not deposed, and the term of the candidate is not adopted
            return new RequestVoteResponse(context.getCurrentTerm(), false);
        }
        if (request.term() > context.getCurrentTerm()) {
            context.stepDown(request.term(), null);
        }

        boolean canVote = context.getVotedFor() == null || context.getVotedFor().equals(request.candidateId());
        long lastLogTerm = context.getRaftLog().getLastLogTerm();
        long lastLogIndex = context.getRaftLog().getLastLogIndex();
        boolean isLogUpToDate = request.lastLogTerm() > lastLogTerm
                || (request.lastLogTerm() == lastLogTerm && request.lastLogIndex() >= lastLogIndex);

        boolean voteGranted = canVote && isLogUpToDate;
        if (voteGranted) {
            // The vote is durable before the response leaves this method
            context.grantVote(request.candidateId());
            context.resetElectionTimer();
            log.info("Voted for {} in term {}", request.candidateId().asString(), request.term());
        }
        return new RequestVoteResponse(context.getCurrentTerm(), voteGranted);
    }

    @Override
    public AppendEntriesResponse handleAppendEntries(RaftState context, AppendEntriesRequest request) {
        if (request.term() < context.getCurrentTerm()) {
            return AppendEntriesResponse.rejected(context.getCurrentTerm(), 0, 0);
        }
        // Adopts a higher term if needed and records the sender as the leader of this term
        context.stepDown(request.term(), request.leaderId());
        context.markLeaderContact();
        preVoteRound++;
        context.resetElectionTimer();

        RaftLog raftLog = context.getRaftLog();
        long prevLogIndex = request.prevLogIndex();

        if (prevLogIndex > raftLog.getLastLogIndex()) {
            return AppendEntriesResponse.rejected(context.getCurrentTerm(), raftLog.getLastLogIndex() + 1, 0);
        }
        if (prevLogIndex > raftLog.getBaseIndex()) {
            long localTerm = raftLog.getEntry(prevLogIndex).term();
            if (localTerm != request.prevLogTerm()) {
                long firstOfTerm = prevLogIndex;
                while (firstOfTerm - 1 > raftLog.getBaseIndex() && raftLog.getEntry(firstOfTerm - 1).term() == localTerm) {
                    firstOfTerm--;
                }
                log.debug("Log mismatch at index {}: local term {}, leader term {}", prevLogIndex, localTerm, request.prevLogTerm());
                return AppendEntriesResponse.rejected(context.getCurrentTerm(), firstOfTerm, localTerm);
            }
        } else if (prevLogIndex == raftLog.getBaseIndex() && prevLogIndex > 0 && request.prevLogTerm() != raftLog.getBaseTerm()) {
            return AppendEntriesResponse.rejected(context.getCurrentTerm(), prevLogIndex, raftLog.getBaseTerm());
        }

        long index = prevLogIndex + 1;
        for (LogEntry entry : request.entries()) {
            if (index > raftLog.getBaseIndex()) {
                LogEntry existing = raftLog.getEntry(index);
                if (existing != null && existing.term() != entry.term()) {
                    raftLog.truncateFrom(index);
                    existing = null;
                }
                if (existing == null) {
                    raftLog.append(entry);
                }
            }
            index++;
        }

        // The entries are durable before they are acknowledged; one flush covers the whole batch
        raftLog.sync();

        if (request.leaderCommit() > context.getCommitIndex()) {
            long lastNewEntryIndex = prevLogIndex + request.entries().size();
            long newCommitIndex = Math.min(request.leaderCommit(), lastNewEntryIndex);
            if (newCommitIndex > context.getCommitIndex()) {
                context.setCommitIndex(newCommitIndex);
                context.applyCommittedEntries();
            }
        }

        return AppendEntriesResponse.accepted(context.getCurrentTerm());
    }

    // Chunks of the snapshot currently being received, identified by the index it ends at
    private long incomingSnapshotIndex = -1;
    private ByteArrayOutputStream incomingSnapshot;

    @Override
    public InstallSnapshotResponse handleInstallSnapshot(RaftState context, InstallSnapshotRequest request) {
        if (request.term() < context.getCurrentTerm()) {
            return new InstallSnapshotResponse(context.getCurrentTerm(), 0, false);
        }
        context.stepDown(request.term(), request.leaderId());
        context.markLeaderContact();
        preVoteRound++;
        context.resetElectionTimer();

        if (request.lastIncludedIndex() <= context.getCommitIndex()) {
            // Everything in the snapshot is already committed here
            incomingSnapshotIndex = -1;
            incomingSnapshot = null;
            return new InstallSnapshotResponse(context.getCurrentTerm(), 0, true);
        }

        if (incomingSnapshotIndex != request.lastIncludedIndex()) {
            incomingSnapshotIndex = request.lastIncludedIndex();
            incomingSnapshot = new ByteArrayOutputStream();
        }
        if (request.offset() != incomingSnapshot.size()) {
            // Tell the leader where to resume instead of accepting a chunk out of order
            return new InstallSnapshotResponse(context.getCurrentTerm(), incomingSnapshot.size(), false);
        }
        incomingSnapshot.writeBytes(request.data());
        if (!request.done()) {
            return new InstallSnapshotResponse(context.getCurrentTerm(), incomingSnapshot.size(), false);
        }

        byte[] serialized = incomingSnapshot.toByteArray();
        incomingSnapshotIndex = -1;
        incomingSnapshot = null;
        context.installSnapshot((Snapshot) context.getSerializer().fromBytes(serialized));
        return new InstallSnapshotResponse(context.getCurrentTerm(), serialized.length, true);
    }

    @Override
    public CompletableFuture<Object> handleClientCommand(RaftState context, ClientCommandRequest request, boolean local) {
        return forward(context, request, local, Duration.ofMillis(context.getConfig().getCommandTimeoutMs()));
    }

    @Override
    public CompletableFuture<Object> handleClientQuery(RaftState context, ClientQueryRequest request, boolean local) {
        if (local && request.consistency() == ReadConsistency.LOCAL) {
            return context.executeQuery(request.query());
        }
        return forward(context, request, local, Duration.ofMillis(context.getConfig().getCommandTimeoutMs()));
    }

    private CompletableFuture<Object> forward(RaftState context, Object request, boolean local, Duration timeout) {
        NodeAddress leader = context.getCurrentLeader();
        if (!local) {
            return CompletableFuture.failedFuture(new KurinNotLeaderException(
                    "Node " + context.getLocalAddress().asString() + " is not the leader",
                    leader != null ? leader.asString() : null));
        }
        if (leader == null) {
            return CompletableFuture.failedFuture(new KurinNoLeaderException("The cluster has no known leader yet"));
        }

        log.debug("Forwarding {} to leader {}", request.getClass().getSimpleName(), leader.asString());
        CompletableFuture<Object> result = new CompletableFuture<>();
        context.getTransport().sendRequest(leader, request, timeout).whenComplete((value, error) -> {
            Throwable cause = error != null ? RpcResult.unwrap(error) : null;
            if (cause instanceof KurinNotLeaderException notLeader
                    && notLeader.leaderHint() != null && !notLeader.leaderHint().equals(leader.asString())) {
                // The leader changed under us: one retry at the node it pointed to, with the same request id
                NodeAddress hinted = NodeAddress.parse(notLeader.leaderHint());
                log.debug("{} is no longer the leader. Retrying at {}", leader.asString(), hinted.asString());
                context.getTransport().sendRequest(hinted, request, timeout).whenComplete((retryValue, retryError) -> {
                    if (retryError != null) {
                        result.completeExceptionally(RpcResult.unwrap(retryError));
                    } else {
                        result.complete(retryValue);
                    }
                });
            } else if (cause != null) {
                result.completeExceptionally(cause);
            } else {
                result.complete(value);
            }
        });
        return result;
    }
}
