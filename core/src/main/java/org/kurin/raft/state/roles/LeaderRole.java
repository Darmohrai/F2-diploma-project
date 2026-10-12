package org.kurin.raft.state.roles;

import org.kurin.api.exception.KurinCommandFailedException;
import org.kurin.api.exception.KurinNotLeaderException;
import org.kurin.api.exception.KurinSerializationException;
import org.kurin.network.model.NodeAddress;
import org.kurin.raft.RaftRole;
import org.kurin.raft.log.LogEntry;
import org.kurin.raft.log.LogEntryType;
import org.kurin.raft.log.RaftLog;
import org.kurin.raft.rpc.AppendEntriesRequest;
import org.kurin.raft.rpc.AppendEntriesResponse;
import org.kurin.raft.rpc.ClientCommandRequest;
import org.kurin.raft.rpc.ClientQueryRequest;
import org.kurin.raft.rpc.RequestVoteRequest;
import org.kurin.raft.rpc.RequestVoteResponse;
import org.kurin.raft.state.CommandOutcome;
import org.kurin.raft.state.NodeRole;
import org.kurin.raft.state.RaftState;
import org.kurin.raft.state.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class LeaderRole implements NodeRole {
    private static final Logger log = LoggerFactory.getLogger(LeaderRole.class);

    /**
     * Replication progress of one follower.
     */
    private static final class Peer {
        /** Next entry the follower is known to need. */
        long nextIndex;
        /** Highest entry known to be stored on the follower. */
        long matchIndex;
        /** Next entry to put on the wire; runs ahead of nextIndex while requests are in flight. */
        long sendIndex;
        int inflight;
        /** Sequence number of the most recent request; answers to older ones must not move nextIndex back. */
        long lastRequestSeq;
    }

    private final Map<NodeAddress, Peer> peers = new HashMap<>();
    private final Map<Long, Integer> entrySizes = new HashMap<>();
    private int maxAppendBytes;

    @Override
    public RaftRole role() {
        return RaftRole.LEADER;
    }

    @Override
    public void onEnter(RaftState context) {
        long lastLogIndex = context.getRaftLog().getLastLogIndex();
        for (NodeAddress address : context.getPeers()) {
            Peer peer = new Peer();
            peer.nextIndex = lastLogIndex + 1;
            peer.sendIndex = lastLogIndex + 1;
            peers.put(address, peer);
        }
        maxAppendBytes = context.getConfig().getMaxAppendBytes();

        context.rebuildUncommittedRequests();
        replicateToAll(context, true);
        context.startHeartbeatTimer();
    }

    @Override
    public void onExit(RaftState context) {
        context.cancelHeartbeatTimer();
        NodeAddress successor = context.getCurrentLeader();
        // The entries may still commit under the next leader: the caller can retry with the same request id
        context.failPendingClientRequests(new KurinNotLeaderException(
                "Node " + context.getLocalAddress().asString() + " lost leadership before the command was committed",
                successor != null && !successor.equals(context.getLocalAddress()) ? successor.asString() : null));
    }

    @Override
    public void onElectionTimeout(RaftState context) {
        // A leader runs no election timer
    }

    @Override
    public void onHeartbeatTick(RaftState context) {
        replicateToAll(context, true);
    }

    private void replicateToAll(RaftState context, boolean heartbeat) {
        for (NodeAddress address : context.getPeers()) {
            replicateTo(context, address, heartbeat);
        }
    }

    private void replicateTo(RaftState context, NodeAddress address, boolean heartbeat) {
        Peer peer = peers.get(address);
        RaftLog raftLog = context.getRaftLog();
        if (peer.inflight >= context.getConfig().getMaxInflightAppends()) {
            return;
        }
        boolean hasEntries = peer.sendIndex <= raftLog.getLastLogIndex();
        if (!hasEntries && !heartbeat) {
            return;
        }

        long prevLogIndex = peer.sendIndex - 1;
        long prevLogTerm = prevLogIndex == raftLog.getBaseIndex() ? raftLog.getBaseTerm() : raftLog.getEntry(prevLogIndex).term();
        List<LogEntry> batch = hasEntries ? nextBatch(context, peer.sendIndex) : List.of();

        AppendEntriesRequest request = new AppendEntriesRequest(
                context.getCurrentTerm(),
                context.getLocalAddress(),
                prevLogIndex,
                prevLogTerm,
                batch,
                context.getCommitIndex()
        );

        long requestSeq = ++peer.lastRequestSeq;
        peer.inflight++;
        peer.sendIndex += batch.size();

        context.getTransport().sendRequest(address, request, context.getRpcTimeout()).whenComplete((response, error) ->
                context.onRaftThread(() -> {
                    if (context.getCurrentRole() != this) {
                        return;
                    }
                    peer.inflight--;
                    if (error != null || !(response instanceof AppendEntriesResponse appendResponse)) {
                        // Whatever was in flight may be lost: resume from the last confirmed position
                        peer.sendIndex = peer.nextIndex;
                        return;
                    }
                    onAppendResponse(context, address, peer, request, requestSeq, appendResponse);
                }));
    }

    private List<LogEntry> nextBatch(RaftState context, long fromIndex) {
        RaftLog raftLog = context.getRaftLog();
        int maxEntries = context.getConfig().getMaxEntriesPerAppend();
        List<LogEntry> batch = new ArrayList<>();
        int bytes = 0;
        for (long index = fromIndex; index <= raftLog.getLastLogIndex() && batch.size() < maxEntries; index++) {
            LogEntry entry = raftLog.getEntry(index);
            int size = sizeOf(context, entry);
            if (!batch.isEmpty() && bytes + size > maxAppendBytes) {
                break;
            }
            batch.add(entry);
            bytes += size;
        }
        return batch;
    }

    private int sizeOf(RaftState context, LogEntry entry) {
        return entrySizes.computeIfAbsent(entry.index(), index -> context.getSerializer().toBytes(entry).length);
    }

    private void onAppendResponse(RaftState context, NodeAddress address, Peer peer, AppendEntriesRequest request,
                                  long requestSeq, AppendEntriesResponse response) {
        if (response.term() > context.getCurrentTerm()) {
            log.info("Peer {} is in term {}. Leader {} steps down.", address.asString(), response.term(),
                    context.getLocalAddress().asString());
            context.stepDown(response.term(), null);
            return;
        }
        if (request.term() != context.getCurrentTerm()) {
            return;
        }

        if (response.success()) {
            long confirmed = request.prevLogIndex() + request.entries().size();
            if (confirmed > peer.matchIndex) {
                peer.matchIndex = confirmed;
                peer.nextIndex = confirmed + 1;
                peer.sendIndex = Math.max(peer.sendIndex, peer.nextIndex);
                advanceCommitIndex(context);
            }
        } else if (requestSeq == peer.lastRequestSeq) {
            // Only the answer to the latest request may move nextIndex back; older answers describe a stale log
            peer.nextIndex = Math.max(peer.matchIndex + 1, nextIndexAfterConflict(context, peer, response));
            peer.sendIndex = peer.nextIndex;
            log.debug("Peer {} rejected entries after {}. Resuming from {}", address.asString(), request.prevLogIndex(), peer.nextIndex);
        }

        if (context.getCurrentRole() == this) {
            replicateTo(context, address, false);
        }
    }

    // Fast backtracking: skip the whole conflicting term instead of one entry per round trip
    private long nextIndexAfterConflict(RaftState context, Peer peer, AppendEntriesResponse response) {
        RaftLog raftLog = context.getRaftLog();
        long candidate = response.conflictIndex();
        if (response.conflictTerm() > 0) {
            for (long index = Math.min(peer.nextIndex - 1, raftLog.getLastLogIndex()); index > raftLog.getBaseIndex(); index--) {
                long term = raftLog.getEntry(index).term();
                if (term == response.conflictTerm()) {
                    candidate = index + 1;
                    break;
                }
                if (term < response.conflictTerm()) {
                    break;
                }
            }
        }
        if (candidate <= 0) {
            candidate = peer.nextIndex - 1;
        }
        return Math.max(raftLog.getBaseIndex() + 1, Math.min(candidate, raftLog.getLastLogIndex() + 1));
    }

    private void advanceCommitIndex(RaftState context) {
        List<Long> matches = new ArrayList<>(peers.size() + 1);
        for (Peer peer : peers.values()) {
            matches.add(peer.matchIndex);
        }
        matches.add(context.getRaftLog().getLastLogIndex());
        matches.sort(Collections.reverseOrder());
        long quorumMatchIndex = matches.get(context.quorumSize() - 1);

        if (quorumMatchIndex > context.getCommitIndex()) {
            LogEntry entry = context.getRaftLog().getEntry(quorumMatchIndex);
            // Only entries of the current term are committed by counting replicas (Raft §5.4.2)
            if (entry != null && entry.term() == context.getCurrentTerm()) {
                context.setCommitIndex(quorumMatchIndex);
                context.applyCommittedEntries();
                entrySizes.keySet().removeIf(index -> index <= minMatchIndex());
            }
        }
    }

    private long minMatchIndex() {
        long min = Long.MAX_VALUE;
        for (Peer peer : peers.values()) {
            min = Math.min(min, peer.matchIndex);
        }
        return min;
    }

    @Override
    public CompletableFuture<Object> handleClientCommand(RaftState context, ClientCommandRequest request, boolean local) {
        String requestId = request.requestId();

        CommandOutcome processed = context.processedOutcome(requestId);
        if (processed != null) {
            log.debug("Request {} was already applied. Returning the remembered result.", requestId);
            return processed.toFuture();
        }
        Long uncommittedIndex = context.uncommittedIndexOf(requestId);
        if (uncommittedIndex != null) {
            log.debug("Request {} is already in the log at index {}. Joining it.", requestId, uncommittedIndex);
            return context.clientFutureFor(uncommittedIndex);
        }

        RaftLog raftLog = context.getRaftLog();
        long newIndex = raftLog.getLastLogIndex() + 1;
        LogEntry entry = new LogEntry(newIndex, context.getCurrentTerm(), LogEntryType.USER, requestId,
                System.currentTimeMillis(), request.command());

        // Measuring the entry up front also rejects commands that cannot be serialized or replicated at all
        int size;
        try {
            size = context.getSerializer().toBytes(entry).length;
        } catch (KurinSerializationException e) {
            return CompletableFuture.failedFuture(e);
        }
        if (size > maxAppendBytes) {
            return CompletableFuture.failedFuture(new KurinSerializationException("Command " + request.command().getClass().getSimpleName()
                    + " takes " + size + " bytes, over the replication limit of " + maxAppendBytes + " bytes"));
        }
        entrySizes.put(newIndex, size);

        raftLog.append(entry);
        context.trackUncommittedRequest(requestId, newIndex);
        CompletableFuture<Object> future = context.clientFutureFor(newIndex);
        log.debug("Request {} appended at index {}", requestId, newIndex);

        if (context.getPeers().isEmpty()) {
            context.setCommitIndex(newIndex);
            context.applyCommittedEntries();
            entrySizes.remove(newIndex);
        } else {
            replicateToAll(context, false);
        }
        return future;
    }

    @Override
    public CompletableFuture<Object> handleClientQuery(RaftState context, ClientQueryRequest request, boolean local) {
        try {
            return CompletableFuture.completedFuture(context.getStateMachine().executeQuery(request.query()));
        } catch (StateMachine.HandlerFailedException e) {
            Throwable cause = e.getCause();
            return CompletableFuture.failedFuture(new KurinCommandFailedException(cause.getClass().getName(), cause.getMessage()));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(new KurinCommandFailedException(e.getClass().getName(), e.getMessage()));
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
        if (request.term() > context.getCurrentTerm()) {
            context.stepDown(request.term(), request.leaderId());
            return context.getCurrentRole().handleAppendEntries(context, request);
        }
        return AppendEntriesResponse.rejected(context.getCurrentTerm(), 0, 0);
    }
}
