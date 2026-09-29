package org.kurin.raft.state;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import org.kurin.network.client.KurinNetworkClient;
import org.kurin.network.mailbox.Mailbox;
import org.kurin.network.model.NodeAddress;
import org.kurin.raft.log.LogEntry;
import org.kurin.raft.log.RaftLog;
import org.kurin.raft.state.roles.CandidateRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class RaftState {

    private static final Logger log = LoggerFactory.getLogger(RaftState.class);

    private KurinNetworkClient networkClient;

    private final StateMachine stateMachine;

    private final NodeAddress localAddress;
    private final Set<NodeAddress> peers;
    private final RaftLog raftLog;
    private final Mailbox mailbox;

    private NodeAddress currentLeader = null;

    private long currentTerm = 0;
    private NodeAddress votedFor = null;

    private long commitIndex = 0;
    private long lastApplied = 0;

    private Timer timer;

    private NodeRole currentRole;

    private Timeout electionTimeout;
    private Timeout heartbeatTimeout;

    private final Map<Long, CompletableFuture<Object>> pendingClientRequests = new HashMap<>();

    private final Map<String, Object> processedRequests = new LinkedHashMap<>(10000, 0.75f, true) { //todo - not hardcode
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
            return size() > 10000;
        }
    };

    private final PersistentMetadata metadata;

    public RaftState(NodeAddress localAddress, Set<NodeAddress> peers, RaftLog raftLog,
                     Mailbox mailbox, StateMachine stateMachine, PersistentMetadata metadata) {
        this.localAddress = localAddress;
        this.peers = peers;
        this.raftLog = raftLog;
        this.mailbox = mailbox;
        this.stateMachine = stateMachine;
        this.metadata = metadata;
    }

    public void transitionTo(NodeRole newRole) {
        if (this.currentRole != null) {
            log.info("Node {} is leaving the role: {}", localAddress.asString(), this.currentRole.roleName());
            this.currentRole.onExit(this);
        }

        this.currentRole = newRole;
        log.info("Node {} transitions to the role: {}", localAddress.asString(), newRole.roleName());
        this.currentRole.onEnter(this);
    }

    public void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = raftLog.getEntry(lastApplied);
            if (entry != null) {
                Object result;
                String reqId = entry.requestId();

                if (reqId != null && processedRequests.containsKey(reqId)) {
                    log.info("Duplicate request {} detected at index {}. Returning cached result.", reqId, lastApplied);
                    result = processedRequests.get(reqId);
                } else {
                    log.info("Applying transaction {} to StateMachine", lastApplied);
                    result = stateMachine.apply(entry.command());

                    if (reqId != null) {
                        processedRequests.put(reqId, result);
                    }
                }

                CompletableFuture<Object> pending = pendingClientRequests.remove(lastApplied);
                if (pending != null) {
                    pending.complete(result);
                }
            }
        }
    }

    public boolean hasProcessedRequest(String requestId) {
        return requestId != null && processedRequests.containsKey(requestId);
    }

    public Object getCachedResult(String requestId) {
        return processedRequests.get(requestId);
    }

    public void resetElectionTimer() {
        cancelElectionTimer();

        long timeoutMs = ThreadLocalRandom.current().nextLong(150, 301);

        this.electionTimeout = timer.newTimeout(t -> {
            mailbox.submit(() -> {
                log.warn("Election timeout ({} ms) triggered! Initiating an election.", timeoutMs);
                transitionTo(new CandidateRole());
            });
        }, timeoutMs, TimeUnit.MILLISECONDS);
    }

    public void cancelElectionTimer() {
        if (this.electionTimeout != null && !this.electionTimeout.isExpired()) {
            this.electionTimeout.cancel();
        }
    }

    public void startHeartbeatTimer() {
        cancelHeartbeatTimer();
        this.heartbeatTimeout = timer.newTimeout(t -> {
            mailbox.submit(() -> {
                if (currentRole instanceof org.kurin.raft.state.roles.LeaderRole leaderRole) {
                    leaderRole.broadcastAppendEntries(this);
                    startHeartbeatTimer();
                }
            });
        }, 50, TimeUnit.MILLISECONDS);
    }

    public void cancelHeartbeatTimer() {
        if (this.heartbeatTimeout != null && !this.heartbeatTimeout.isExpired()) {
            this.heartbeatTimeout.cancel();
        }
    }

    public void registerClientFuture(long logIndex, CompletableFuture<Object> future) {
        pendingClientRequests.put(logIndex, future);
    }

    public void clearPendingClientRequests(Throwable reason) {
        for (CompletableFuture<Object> future : pendingClientRequests.values()) {
            future.completeExceptionally(reason);
        }
        pendingClientRequests.clear();
    }

    public void triggerSnapshot() {
        mailbox.submit(() -> {
            long lastIndex = lastApplied;
            if (lastIndex <= raftLog.getBaseIndex()) {
                return;
            }

            log.info("Triggering snapshot generation up to index {}", lastIndex);

            byte[] snapshotData = stateMachine.takeSnapshot();

            LogEntry entry = raftLog.getEntry(lastIndex);
            long term = (entry != null) ? entry.term() : currentTerm;
            raftLog.compact(lastIndex, term);

            log.info("Snapshot successfully generated and log compacted.");
        });
    }

    // --- Getters & Setters ---

    public long getCurrentTerm() {
        return metadata.getCurrentTerm();
    }

    public void setCurrentTerm(long term) {
        metadata.setCurrentTerm(term);
    }

    public NodeAddress getVotedFor() {
        return metadata.getVotedFor();
    }

    public void setVotedFor(NodeAddress votedFor) {
        metadata.setVotedFor(votedFor);
    }

    public void flushMetadata() {
        metadata.flush();
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }

    public NodeRole getCurrentRole() {
        return currentRole;
    }

    public NodeAddress getLocalAddress() {
        return localAddress;
    }

    public Set<NodeAddress> getPeers() {
        return peers;
    }

    public RaftLog getRaftLog() {
        return raftLog;
    }

    public void setNetworkClient(KurinNetworkClient networkClient) {
        this.networkClient = networkClient;
    }

    public KurinNetworkClient getNetworkClient() {
        return networkClient;
    }

    public Mailbox getMailbox() {
        return mailbox;
    }

    public void setTimer(Timer timer) {
        this.timer = timer;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public NodeAddress getCurrentLeader() {
        return currentLeader;
    }

    public void setCurrentLeader(NodeAddress currentLeader) {
        this.currentLeader = currentLeader;
    }
}
