package org.kurin.raft.state.roles;

import org.kurin.api.LeadershipToken;
import org.kurin.api.ReadConsistency;
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
import org.kurin.raft.rpc.InstallSnapshotRequest;
import org.kurin.raft.rpc.InstallSnapshotResponse;
import org.kurin.raft.snapshot.Snapshot;
import java.util.Arrays;
import org.kurin.raft.rpc.RequestVoteRequest;
import org.kurin.raft.rpc.RequestVoteResponse;
import org.kurin.raft.state.CommandOutcome;
import org.kurin.raft.state.NodeRole;
import org.kurin.raft.state.RaftState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class LeaderRole implements NodeRole, RaftState.LeaseHolder {
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
        /** When the latest answered request was sent. An answer proves the peer followed us at that moment. */
        long lastAckSentNanos;
        boolean acked;
        /** Snapshot being sent to a follower that is behind the start of the log, or null. */
        SnapshotTransfer transfer;
    }

    private static final class SnapshotTransfer {
        final long lastIncludedIndex;
        final long lastIncludedTerm;
        final byte[] bytes;
        int offset;

        SnapshotTransfer(long lastIncludedIndex, long lastIncludedTerm, byte[] bytes) {
            this.lastIncludedIndex = lastIncludedIndex;
            this.lastIncludedTerm = lastIncludedTerm;
            this.bytes = bytes;
        }
    }

    /**
     * A linearizable read waiting for proof that this node was still the leader after the read arrived.
     */
    private record PendingRead(Object query, long arrivedNanos, CompletableFuture<Object> future) {
    }

    private final Map<NodeAddress, Peer> peers = new HashMap<>();
    private final Map<Long, Integer> entrySizes = new HashMap<>();
    // When this leader appended the entries that are not committed yet, for the commit latency
    private final Map<Long, Long> appendedAtNanos = new HashMap<>();
    private final List<PendingRead> pendingReads = new ArrayList<>();
    private final List<Runnable> awaitingNoop = new ArrayList<>();
    private int maxAppendBytes;
    private long noopIndex;
    private long enteredNanos;
    private long quorumTimeoutNanos;
    private long leaseDurationNanos;
    // The stored snapshot in wire form, serialized once per snapshot rather than once per chunk or heartbeat
    private long serializedSnapshotIndex = -1;
    private byte[] serializedSnapshot;

    @Override
    public RaftRole role() {
        return RaftRole.LEADER;
    }

    @Override
    public void onEnter(RaftState context) {
        RaftLog raftLog = context.getRaftLog();
        long lastLogIndex = raftLog.getLastLogIndex();
        enteredNanos = System.nanoTime();
        quorumTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(context.getConfig().getElectionTimeoutMinMs());
        leaseDurationNanos = (long) (quorumTimeoutNanos * context.getConfig().getLeaseRatio());
        maxAppendBytes = context.getConfig().getMaxAppendBytes();

        for (NodeAddress address : context.getPeers()) {
            Peer peer = new Peer();
            peer.nextIndex = lastLogIndex + 1;
            peer.sendIndex = lastLogIndex + 1;
            // Grace period for check-quorum: a fresh leader is not expected to have acknowledgements yet
            peer.lastAckSentNanos = enteredNanos;
            peers.put(address, peer);
        }
        context.rebuildUncommittedRequests();

        // Entries of earlier terms are committed only together with an entry of this term (Raft §5.4.2),
        // so the new leader writes one right away. Until it commits, the leader serves no reads and holds no lease.
        noopIndex = lastLogIndex + 1;
        raftLog.append(new LogEntry(noopIndex, context.getCurrentTerm(), LogEntryType.NOOP, null, System.currentTimeMillis(), null));

        replicateToAll(context, true);
        advanceCommitIndex(context);
        context.startHeartbeatTimer();
    }

    @Override
    public void onExit(RaftState context) {
        context.cancelHeartbeatTimer();
        context.setLeadershipToken(null);

        NodeAddress successor = context.getCurrentLeader();
        String hint = successor != null && !successor.equals(context.getLocalAddress()) ? successor.asString() : null;
        String self = context.getLocalAddress().asString();
        // The entries may still commit under the next leader: the caller can retry with the same request id
        context.failPendingClientRequests(new KurinNotLeaderException(
                "Node " + self + " lost leadership before the command was committed", hint));

        KurinNotLeaderException readFailure = new KurinNotLeaderException("Node " + self + " lost leadership before the query was answered", hint);
        pendingReads.forEach(read -> read.future().completeExceptionally(readFailure));
        pendingReads.clear();
        List<Runnable> waiting = new ArrayList<>(awaitingNoop);
        awaitingNoop.clear();
        // Each waiting read re-checks the role and fails itself
        waiting.forEach(Runnable::run);
    }

    @Override
    public void onElectionTimeout(RaftState context) {
        // A leader runs no election timer
    }

    @Override
    public void onHeartbeatTick(RaftState context) {
        // Check-quorum: a leader that cannot reach a majority stops being one, so that clients stop waiting
        // on it and its lease provably ends before another leader can be elected
        if (!context.getPeers().isEmpty() && quorumAckAgeNanos(false) > quorumTimeoutNanos) {
            log.warn("Leader {} has not heard from a majority for {} ms. Stepping down.",
                    context.getLocalAddress().asString(), context.getConfig().getElectionTimeoutMinMs());
            context.stepDown(context.getCurrentTerm(), null);
            return;
        }
        replicateToAll(context, true);
    }

    // ---------------------------------------------------------------- quorum contact and lease

    /**
     * @param confirmedOnly true to ignore peers that have not answered since this node became leader
     * @return how long ago the heartbeat round acknowledged by a majority (this node included) was sent;
     * {@link Long#MAX_VALUE} if no such round exists yet
     */
    private long quorumAckAgeNanos(boolean confirmedOnly) {
        if (peers.isEmpty()) {
            return 0;
        }
        long now = System.nanoTime();
        List<Long> ages = new ArrayList<>(peers.size());
        for (Peer peer : peers.values()) {
            ages.add(confirmedOnly && !peer.acked ? Long.MAX_VALUE : now - peer.lastAckSentNanos);
        }
        Collections.sort(ages);
        // This node always counts towards the majority, so quorumSize - 1 peers are needed
        int quorumSize = (peers.size() + 1) / 2 + 1;
        return ages.get(quorumSize - 2);
    }

    private boolean noopCommitted(RaftState context) {
        return context.getCommitIndex() >= noopIndex;
    }

    @Override
    public long leaseValidUntilNanos() {
        return leaseValidUntil;
    }

    private long leaseValidUntil;

    // The lease runs from the moment the acknowledged heartbeats were sent, not from when the answers arrived
    private void refreshLease(RaftState context) {
        if (!noopCommitted(context)) {
            leaseValidUntil = 0;
            return;
        }
        long age = quorumAckAgeNanos(true);
        if (age == Long.MAX_VALUE) {
            leaseValidUntil = 0;
            return;
        }
        long validUntil = System.nanoTime() - age + leaseDurationNanos;
        leaseValidUntil = validUntil == 0 ? 1 : validUntil; // 0 is reserved for "no lease"
    }

    private boolean leaseValid(RaftState context) {
        refreshLease(context);
        return leaseValidUntil != 0 && leaseValidUntil - System.nanoTime() > 0;
    }

    @Override
    public boolean hasLiveLeader(RaftState context) {
        return leaseValid(context);
    }

    @Override
    public Map<String, Long> peerContactNanos(RaftState context) {
        Map<String, Long> contacts = new HashMap<>();
        peers.forEach((address, peer) -> {
            if (peer.acked) {
                contacts.put(address.asString(), peer.lastAckSentNanos);
            }
        });
        return contacts;
    }

    // ---------------------------------------------------------------- replication

    private void replicateToAll(RaftState context, boolean heartbeat) {
        if (peers.isEmpty()) {
            refreshLease(context);
            return;
        }
        for (NodeAddress address : context.getPeers()) {
            replicateTo(context, address, heartbeat);
        }
    }

    private void replicateTo(RaftState context, NodeAddress address, boolean heartbeat) {
        Peer peer = peers.get(address);
        RaftLog raftLog = context.getRaftLog();
        if (peer.nextIndex <= raftLog.getBaseIndex()) {
            // The entries this follower needs were compacted away: it gets the snapshot instead
            sendSnapshotChunk(context, address, peer);
            return;
        }
        peer.transfer = null;
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
        long sentNanos = System.nanoTime();
        peer.inflight++;
        peer.sendIndex += batch.size();
        if (!batch.isEmpty()) {
            int batchSize = batch.size();
            context.observe(observer -> observer.onAppendBatch(batchSize));
        }

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
                    onAppendResponse(context, address, peer, request, requestSeq, sentNanos, appendResponse);
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
                                  long requestSeq, long sentNanos, AppendEntriesResponse response) {
        if (response.term() > context.getCurrentTerm()) {
            log.info("Peer {} is in term {}. Leader {} steps down.", address.asString(), response.term(),
                    context.getLocalAddress().asString());
            context.stepDown(response.term(), null);
            return;
        }
        if (request.term() != context.getCurrentTerm()) {
            return;
        }

        // Accepted or not, the peer answered in our term: it considered us the leader when the request was sent
        if (!peer.acked || sentNanos - peer.lastAckSentNanos > 0) {
            peer.lastAckSentNanos = sentNanos;
            peer.acked = true;
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

        if (context.getCurrentRole() != this) {
            return;
        }
        refreshLease(context);
        publishLeadershipIfEstablished(context);
        completeConfirmedReads(context);
        replicateTo(context, address, false);
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
        // A result at or below the base index makes the next round send the snapshot instead of entries
        return Math.max(1, Math.min(candidate, raftLog.getLastLogIndex() + 1));
    }

    private void advanceCommitIndex(RaftState context) {
        List<Long> matches = new ArrayList<>(peers.size() + 1);
        for (Peer peer : peers.values()) {
            matches.add(peer.matchIndex);
        }
        // The leader counts itself only for entries that are already on its own disk
        matches.add(context.getRaftLog().getDurableIndex());
        matches.sort(Collections.reverseOrder());
        long quorumMatchIndex = matches.get(context.quorumSize() - 1);

        if (quorumMatchIndex > context.getCommitIndex()) {
            LogEntry entry = context.getRaftLog().getEntry(quorumMatchIndex);
            // Only entries of the current term are committed by counting replicas (Raft §5.4.2)
            if (entry != null && entry.term() == context.getCurrentTerm()) {
                commitTo(context, quorumMatchIndex);
            }
        }
    }

    private void commitTo(RaftState context, long index) {
        boolean noopWasCommitted = noopCommitted(context);
        long committedNanos = System.nanoTime();
        appendedAtNanos.entrySet().removeIf(appended -> {
            if (appended.getKey() > index) {
                return false;
            }
            long latencyNanos = committedNanos - appended.getValue();
            context.observe(observer -> observer.onCommit(latencyNanos));
            return true;
        });
        context.setCommitIndex(index);
        context.applyCommittedEntries();

        long replicatedEverywhere = Long.MAX_VALUE;
        for (Peer peer : peers.values()) {
            replicatedEverywhere = Math.min(replicatedEverywhere, peer.matchIndex);
        }
        long obsolete = Math.min(replicatedEverywhere, index);
        entrySizes.keySet().removeIf(entryIndex -> entryIndex <= obsolete);

        if (!noopWasCommitted && noopCommitted(context)) {
            log.info("Leadership of {} in term {} is established at index {}", context.getLocalAddress().asString(),
                    context.getCurrentTerm(), noopIndex);
            refreshLease(context);
            publishLeadershipIfEstablished(context);
            List<Runnable> waiting = new ArrayList<>(awaitingNoop);
            awaitingNoop.clear();
            waiting.forEach(Runnable::run);
        }
    }

    private void publishLeadershipIfEstablished(RaftState context) {
        if (context.leadershipToken() == null && leaseValidUntil != 0) {
            context.setLeadershipToken(new LeadershipToken(context.getCurrentTerm(), context.getLocalAddress().asString(), noopIndex));
        }
    }

    // ---------------------------------------------------------------- commands

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
        appendedAtNanos.put(newIndex, System.nanoTime());
        context.rememberTraceHeaders(newIndex, request.headers());

        raftLog.append(entry);
        context.trackUncommittedRequest(requestId, newIndex);
        CompletableFuture<Object> future = context.clientFutureFor(newIndex);
        log.debug("Request {} appended at index {}", requestId, newIndex);

        replicateToAll(context, false);
        advanceCommitIndex(context);
        return future;
    }

    // ---------------------------------------------------------------- queries

    @Override
    public CompletableFuture<Object> handleClientQuery(RaftState context, ClientQueryRequest request, boolean local) {
        ReadConsistency consistency = request.consistency() != null ? request.consistency() : ReadConsistency.LINEARIZABLE;
        if (consistency == ReadConsistency.LOCAL) {
            return context.executeQuery(request.query());
        }
        if (consistency == ReadConsistency.LEASE) {
            if (leaseValid(context)) {
                return context.executeQuery(request.query());
            }
            return CompletableFuture.failedFuture(new KurinNotLeaderException(
                    "Leader " + context.getLocalAddress().asString() + " holds no valid lease right now", null));
        }

        CompletableFuture<Object> future = new CompletableFuture<>();
        if (noopCommitted(context)) {
            startLinearizableRead(context, request.query(), future);
        } else {
            // The commit index of a new leader is trustworthy only after its first entry commits
            awaitingNoop.add(() -> {
                if (context.getCurrentRole() == this && noopCommitted(context)) {
                    startLinearizableRead(context, request.query(), future);
                } else {
                    future.completeExceptionally(new KurinNotLeaderException(
                            "Node " + context.getLocalAddress().asString() + " lost leadership before the query was answered", null));
                }
            });
        }
        return future;
    }

    /**
     * ReadIndex: the state at the current commit index may be returned once this node is known to have been
     * the leader at some point after the read arrived. On a leader every committed entry is applied at once,
     * so there is nothing further to wait for.
     */
    private void startLinearizableRead(RaftState context, Object query, CompletableFuture<Object> future) {
        if (leaseValid(context)) {
            completeRead(context, query, future);
            return;
        }
        pendingReads.add(new PendingRead(query, System.nanoTime(), future));
        replicateToAll(context, true);
    }

    private void completeConfirmedReads(RaftState context) {
        if (pendingReads.isEmpty()) {
            return;
        }
        long ackAge = quorumAckAgeNanos(true);
        if (ackAge == Long.MAX_VALUE) {
            return;
        }
        long quorumAckSentNanos = System.nanoTime() - ackAge;
        for (Iterator<PendingRead> iterator = pendingReads.iterator(); iterator.hasNext(); ) {
            PendingRead read = iterator.next();
            // A majority answered a heartbeat sent after the read arrived
            if (quorumAckSentNanos - read.arrivedNanos() >= 0) {
                iterator.remove();
                completeRead(context, read.query(), read.future());
            }
        }
    }

    private void completeRead(RaftState context, Object query, CompletableFuture<Object> future) {
        context.executeQuery(query).whenComplete((value, error) -> {
            if (error != null) {
                future.completeExceptionally(error);
            } else {
                future.complete(value);
            }
        });
    }

    // ---------------------------------------------------------------- snapshot transfer

    // One chunk at a time per follower; the next one is sent when this one is acknowledged
    private void sendSnapshotChunk(RaftState context, NodeAddress address, Peer peer) {
        if (peer.inflight > 0) {
            return;
        }
        Snapshot snapshot = context.latestSnapshot();
        if (peer.transfer == null || peer.transfer.lastIncludedIndex != snapshot.lastIncludedIndex()) {
            if (serializedSnapshotIndex != snapshot.lastIncludedIndex()) {
                serializedSnapshot = context.getSerializer().toBytes(snapshot);
                serializedSnapshotIndex = snapshot.lastIncludedIndex();
            }
            peer.transfer = new SnapshotTransfer(snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(), serializedSnapshot);
            log.info("Sending snapshot up to index {} ({} bytes) to {}", snapshot.lastIncludedIndex(),
                    serializedSnapshot.length, address.asString());
        }
        SnapshotTransfer transfer = peer.transfer;
        int length = Math.min(maxAppendBytes, transfer.bytes.length - transfer.offset);
        byte[] chunk = Arrays.copyOfRange(transfer.bytes, transfer.offset, transfer.offset + length);
        boolean done = transfer.offset + length >= transfer.bytes.length;

        InstallSnapshotRequest request = new InstallSnapshotRequest(context.getCurrentTerm(), context.getLocalAddress(),
                transfer.lastIncludedIndex, transfer.lastIncludedTerm, transfer.offset, chunk, done);
        long sentNanos = System.nanoTime();
        peer.inflight++;

        context.getTransport().sendRequest(address, request, context.getRpcTimeout()).whenComplete((response, error) ->
                context.onRaftThread(() -> {
                    if (context.getCurrentRole() != this) {
                        return;
                    }
                    peer.inflight--;
                    if (error != null || !(response instanceof InstallSnapshotResponse snapshotResponse)) {
                        return; // the same chunk is sent again on the next heartbeat
                    }
                    if (snapshotResponse.term() > context.getCurrentTerm()) {
                        context.stepDown(snapshotResponse.term(), null);
                        return;
                    }
                    if (request.term() != context.getCurrentTerm()) {
                        return;
                    }
                    if (!peer.acked || sentNanos - peer.lastAckSentNanos > 0) {
                        peer.lastAckSentNanos = sentNanos;
                        peer.acked = true;
                    }
                    if (peer.transfer == transfer) {
                        if (snapshotResponse.installed()) {
                            peer.matchIndex = Math.max(peer.matchIndex, transfer.lastIncludedIndex);
                            peer.nextIndex = Math.max(peer.nextIndex, transfer.lastIncludedIndex + 1);
                            peer.sendIndex = peer.nextIndex;
                            peer.transfer = null;
                            log.info("Peer {} installed the snapshot up to index {}", address.asString(), transfer.lastIncludedIndex);
                        } else {
                            transfer.offset = Math.max(0, Math.min(snapshotResponse.nextOffset(), transfer.bytes.length));
                        }
                    }
                    refreshLease(context);
                    publishLeadershipIfEstablished(context);
                    completeConfirmedReads(context);
                    replicateTo(context, address, false);
                }));
    }

    @Override
    public void onLogDurable(RaftState context) {
        advanceCommitIndex(context);
    }

    @Override
    public InstallSnapshotResponse handleInstallSnapshot(RaftState context, InstallSnapshotRequest request) {
        if (request.term() > context.getCurrentTerm()) {
            context.stepDown(request.term(), request.leaderId());
            return context.getCurrentRole().handleInstallSnapshot(context, request);
        }
        return new InstallSnapshotResponse(context.getCurrentTerm(), 0, false);
    }

    // ---------------------------------------------------------------- RPCs from other nodes

    @Override
    public RequestVoteResponse handleRequestVote(RaftState context, RequestVoteRequest request) {
        if (request.term() > context.getCurrentTerm() && !leaseValid(context)) {
            context.stepDown(request.term(), null);
            return context.getCurrentRole().handleRequestVote(context, request);
        }
        // A leader in contact with a majority does not yield to a candidate
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
