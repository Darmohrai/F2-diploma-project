package org.kurin.raft.state.roles;

import org.kurin.network.model.NodeAddress;
import org.kurin.raft.log.LogEntry;
import org.kurin.raft.rpc.*;
import org.kurin.raft.state.NodeRole;
import org.kurin.raft.state.RaftState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class LeaderRole implements NodeRole {
    private static final Logger log = LoggerFactory.getLogger(LeaderRole.class);

    private final Map<NodeAddress, Long> nextIndex = new HashMap<>();
    private final Map<NodeAddress, Long> matchIndex = new HashMap<>();

    @Override
    public String roleName() {
        return "LEADER";
    }

    @Override
    public void onEnter(RaftState context) {
        long lastLogIndex = context.getRaftLog().getLastLogIndex();

        for (NodeAddress peer : context.getPeers()) {
            nextIndex.put(peer, lastLogIndex + 1);
            matchIndex.put(peer, 0L);
        }

        broadcastAppendEntries(context);
        context.startHeartbeatTimer();
    }

    @Override
    public void onExit(RaftState context) {
        context.cancelHeartbeatTimer();
        context.clearPendingClientRequests(
                new IllegalStateException("Leader stepped down before committing the command.")
        );
    }

    public void broadcastAppendEntries(RaftState context) {
        for (NodeAddress peer : context.getPeers()) {
            long peerNextIndex = nextIndex.get(peer);

            if (peerNextIndex <= context.getRaftLog().getBaseIndex()) {
                sendInstallSnapshot(context, peer);
                continue;
            }

            long prevLogIndex = peerNextIndex - 1;
            long prevLogTerm = 0;

            if (prevLogIndex == context.getRaftLog().getBaseIndex()) {
                prevLogTerm = context.getRaftLog().getBaseTerm();
            } else if (prevLogIndex > context.getRaftLog().getBaseIndex()) {
                LogEntry prevEntry = context.getRaftLog().getEntry(prevLogIndex);
                if (prevEntry != null) {
                    prevLogTerm = prevEntry.term();
                }
            }

            List<LogEntry> entriesToSend = context.getRaftLog().getEntriesFrom(peerNextIndex);

            AppendEntriesRequest request = new AppendEntriesRequest(
                    context.getCurrentTerm(),
                    context.getLocalAddress(),
                    prevLogIndex,
                    prevLogTerm,
                    entriesToSend,
                    context.getCommitIndex()
            );

            context.getNetworkClient().sendRequest(peer, request).whenComplete((response, error) -> {
                context.getMailbox().submit(() -> {
                    if (context.getCurrentRole() != this || context.getCurrentTerm() != request.term()) {
                        return; // Stale response
                    }
                    if (error == null && response instanceof AppendEntriesResponse appendResponse) {
                        processAppendEntriesResponse(context, peer, appendResponse, entriesToSend.size(), request.prevLogIndex());
                    }
                });
            });
        }
    }

    private void sendInstallSnapshot(RaftState context, NodeAddress peer) {
        byte[] snapshotData = context.getStateMachine().takeSnapshot();

        InstallSnapshotRequest request = new InstallSnapshotRequest(
                context.getCurrentTerm(),
                context.getLocalAddress(),
                context.getRaftLog().getBaseIndex(),
                context.getRaftLog().getBaseTerm(),
                snapshotData
        );

        context.getNetworkClient().sendRequest(peer, request).whenComplete((response, error) -> {
            context.getMailbox().submit(() -> {
                if (context.getCurrentRole() != this || context.getCurrentTerm() != request.term()) return;

                if (error == null && response instanceof InstallSnapshotResponse snapResponse) {
                    if (snapResponse.term() > context.getCurrentTerm()) {
                        context.setCurrentTerm(snapResponse.term());
                        context.transitionTo(new FollowerRole());
                    } else {
                        matchIndex.put(peer, request.lastIncludedIndex());
                        nextIndex.put(peer, request.lastIncludedIndex() + 1);
                    }
                }
            });
        });
    }

    private void processAppendEntriesResponse(RaftState context, NodeAddress peer, AppendEntriesResponse response, int entriesSent, long prevLogIndex) {
        if (response.term() > context.getCurrentTerm()) {
            log.info("Discovered higher term ({}). Stepping down from LEADER.", response.term());
            context.setCurrentTerm(response.term());
            context.transitionTo(new FollowerRole());
            return;
        }

        if (response.success()) {
            long newMatchIndex = prevLogIndex + entriesSent;
            if (newMatchIndex > matchIndex.get(peer)) {
                matchIndex.put(peer, newMatchIndex);
                nextIndex.put(peer, newMatchIndex + 1);
                updateCommitIndex(context);
            }
        } else {
            // Backtracking: decrement nextIndex and retry in next heartbeat
            long currentNextIndex = nextIndex.get(peer);
            if (currentNextIndex > 1) {
                nextIndex.put(peer, currentNextIndex - 1);
            }
        }
    }

    private void updateCommitIndex(RaftState context) {
        List<Long> matches = new ArrayList<>(matchIndex.values());
        matches.add(context.getRaftLog().getLastLogIndex());
        matches.sort(Collections.reverseOrder());

        int majorityIndex = matches.size() / 2;
        long quorumMatchIndex = matches.get(majorityIndex);

        if (quorumMatchIndex > context.getCommitIndex()) {
            LogEntry entry = context.getRaftLog().getEntry(quorumMatchIndex);
            if (entry != null && entry.term() == context.getCurrentTerm()) {
                context.setCommitIndex(quorumMatchIndex);
                log.info("Leader committed transactions up to index {}", quorumMatchIndex);
                context.applyCommittedEntries();
            }
        }
    }

    @Override
    public CompletableFuture<Object> handleClientCommand(RaftState context, ClientCommandRequest request) {
        if (context.hasProcessedRequest(request.requestId())) {
            log.info("Request {} already processed. Returning cached result immediately.", request.requestId());
            return CompletableFuture.completedFuture(context.getCachedResult(request.requestId()));
        }
        long newIndex = context.getRaftLog().getLastLogIndex() + 1;
        long currentTerm = context.getCurrentTerm();
        LogEntry entry = new LogEntry(newIndex, currentTerm, request.requestId(), request.command());
        context.getRaftLog().append(entry);
        log.info("Client command {} received. Recorded at index {}.", request.requestId(), newIndex);

        CompletableFuture<Object> future = new CompletableFuture<>();
        context.registerClientFuture(newIndex, future);

        if (context.getPeers().isEmpty()) {
            context.setCommitIndex(newIndex);
            context.applyCommittedEntries();
        } else {
            broadcastAppendEntries(context);
            context.startHeartbeatTimer();
        }

        return future;
    }

    @Override
    public RequestVoteResponse handleRequestVote(RaftState context, RequestVoteRequest request) {
        if (request.term() > context.getCurrentTerm()) {
            context.setCurrentTerm(request.term());
            context.transitionTo(new FollowerRole());
            return context.getCurrentRole().handleRequestVote(context, request);
        }
        return new RequestVoteResponse(context.getCurrentTerm(), false);
    }

    @Override
    public AppendEntriesResponse handleAppendEntries(RaftState context, AppendEntriesRequest request) {
        if (request.term() > context.getCurrentTerm()) {
            context.setCurrentTerm(request.term());
            context.transitionTo(new FollowerRole());
            return context.getCurrentRole().handleAppendEntries(context, request);
        }
        return new AppendEntriesResponse(context.getCurrentTerm(), false);
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
