package org.kurin.raft.state;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import org.kurin.api.KurinCommandContext;
import org.kurin.api.exception.KurinNotStartedException;
import org.kurin.network.client.KurinTransport;
import org.kurin.network.model.NodeAddress;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.raft.RaftConfig;
import org.kurin.raft.RaftRole;
import org.kurin.raft.RaftStatus;
import org.kurin.raft.log.LogEntry;
import org.kurin.raft.log.LogEntryType;
import org.kurin.raft.log.RaftLog;
import org.kurin.raft.state.roles.FollowerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * All mutable Raft state of a node. It is confined to a single thread, the Raft thread: every method
 * except {@link #execute}, {@link #status()} and the getters of immutable configuration must be called on it.
 * Other threads read the published {@link RaftStatus}.
 */
public class RaftState {
    private static final Logger log = LoggerFactory.getLogger(RaftState.class);
    private static final boolean THREAD_CHECKS = Boolean.getBoolean("kurin.debug.thread-checks");

    private final RaftConfig config;
    private final NodeAddress localAddress;
    private final Set<NodeAddress> peers;
    private final RaftLog raftLog;
    private final StateMachine stateMachine;
    private final PersistentMetadata metadata;
    private final KryoSerializer serializer;
    private final List<RoleChangeListener> roleListeners;
    private final ExecutorService raftExecutor;
    private final Thread raftThread;
    private final Duration rpcTimeout;

    private KurinTransport transport;
    private Timer timer;

    private NodeAddress currentLeader;
    private long commitIndex;
    private long lastApplied;
    private NodeRole currentRole;

    private long electionEpoch;
    private long heartbeatEpoch;
    private Timeout electionTimeout;
    private Timeout heartbeatTimeout;

    private final Map<Long, CompletableFuture<Object>> pendingClientRequests = new HashMap<>();
    // Requests that are in the log but not applied yet: a resubmission joins the entry instead of appending again
    private final Map<String, Long> uncommittedRequests = new HashMap<>();
    // Insertion-ordered so that every node evicts the same entries
    private final LinkedHashMap<String, CommandOutcome> processedRequests;

    private volatile RaftStatus status;

    public RaftState(RaftConfig config, NodeAddress localAddress, Set<NodeAddress> peers, RaftLog raftLog,
                     StateMachine stateMachine, PersistentMetadata metadata, KryoSerializer serializer,
                     List<RoleChangeListener> roleListeners, ExecutorService raftExecutor, Thread raftThread) {
        this.config = config;
        this.localAddress = localAddress;
        this.peers = Set.copyOf(peers);
        this.raftLog = raftLog;
        this.stateMachine = stateMachine;
        this.metadata = metadata;
        this.serializer = serializer;
        this.roleListeners = List.copyOf(roleListeners);
        this.raftExecutor = raftExecutor;
        this.raftThread = raftThread;
        this.rpcTimeout = Duration.ofMillis(config.getElectionTimeoutMinMs());
        int dedupCacheSize = config.getDedupCacheSize();
        this.processedRequests = new LinkedHashMap<>(256, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, CommandOutcome> eldest) {
                return size() > dedupCacheSize;
            }
        };
        this.status = new RaftStatus(RaftRole.FOLLOWER, metadata.getCurrentTerm(), null, 0, 0,
                raftLog.getLastLogIndex(), 0, localAddress.asString());
    }

    // ---------------------------------------------------------------- threading

    /**
     * Runs the task on the Raft thread and publishes a fresh {@link RaftStatus} afterwards.
     *
     * @throws KurinNotStartedException if the node is shut down
     */
    public void execute(Runnable task) {
        try {
            raftExecutor.execute(() -> {
                try {
                    task.run();
                } catch (Throwable t) {
                    // The Raft thread must survive: a dead thread would silently freeze the node
                    log.error("Unexpected error on the Raft thread of {}", localAddress.asString(), t);
                } finally {
                    publishStatus();
                }
            });
        } catch (RejectedExecutionException e) {
            throw new KurinNotStartedException("Node " + localAddress.asString() + " is shut down");
        }
    }

    public void assertOnRaftThread() {
        if (THREAD_CHECKS && Thread.currentThread() != raftThread) {
            throw new IllegalStateException("Raft state of " + localAddress.asString() + " accessed from thread '"
                    + Thread.currentThread().getName() + "' instead of '" + raftThread.getName() + "'");
        }
    }

    /**
     * @return the last published snapshot of the node state; safe to call from any thread
     */
    public RaftStatus status() {
        return status;
    }

    private void publishStatus() {
        RaftRole role = currentRole != null ? currentRole.role() : RaftRole.FOLLOWER;
        long lease = currentRole instanceof LeaseHolder holder ? holder.leaseValidUntilNanos() : 0;
        RaftStatus fresh = new RaftStatus(role, metadata.getCurrentTerm(),
                currentLeader != null ? currentLeader.asString() : null,
                commitIndex, lastApplied, raftLog.getLastLogIndex(), lease, localAddress.asString());
        if (!fresh.equals(status)) {
            status = fresh;
        }
    }

    /**
     * Implemented by the role that may hold a leader lease.
     */
    public interface LeaseHolder {
        long leaseValidUntilNanos();
    }

    // ---------------------------------------------------------------- roles and terms

    public void transitionTo(NodeRole newRole) {
        assertOnRaftThread();
        boolean wasLeader = currentRole != null && currentRole.role() == RaftRole.LEADER;
        boolean isLeader = newRole.role() == RaftRole.LEADER;

        if (currentRole != null) {
            currentRole.onExit(this);
        }
        log.info("Node {} becomes {} in term {}", localAddress.asString(), newRole.role(), getCurrentTerm());
        currentRole = newRole;
        if (isLeader) {
            currentLeader = localAddress;
        }
        currentRole.onEnter(this);

        if (wasLeader != isLeader && currentRole == newRole) {
            publishStatus();
            RaftStatus snapshot = status;
            for (RoleChangeListener listener : roleListeners) {
                try {
                    if (isLeader) {
                        listener.onLeaderElected(snapshot);
                    } else {
                        listener.onLeaderLost(snapshot);
                    }
                } catch (RuntimeException e) {
                    log.warn("Role change listener {} failed", listener.getClass().getName(), e);
                }
            }
        }
    }

    /**
     * The single way to yield to a higher term or to a legitimate leader: adopts the term (clearing the vote
     * and persisting both), records the leader and becomes a follower.
     *
     * @param leaderHint the leader that caused the step-down, or null if it is not known
     */
    public void stepDown(long newTerm, NodeAddress leaderHint) {
        assertOnRaftThread();
        if (newTerm > getCurrentTerm()) {
            metadata.save(newTerm, null);
        }
        currentLeader = leaderHint;
        if (!(currentRole instanceof FollowerRole)) {
            transitionTo(new FollowerRole());
        }
    }

    /**
     * Starts a new election term: increments the term and votes for this node, durably.
     */
    public void beginElectionTerm() {
        assertOnRaftThread();
        metadata.save(getCurrentTerm() + 1, localAddress);
        currentLeader = null;
    }

    /**
     * Records a vote in the current term, durably.
     */
    public void grantVote(NodeAddress candidate) {
        assertOnRaftThread();
        metadata.save(getCurrentTerm(), candidate);
    }

    public long getCurrentTerm() {
        return metadata.getCurrentTerm();
    }

    public NodeAddress getVotedFor() {
        return metadata.getVotedFor();
    }

    public NodeRole getCurrentRole() {
        return currentRole;
    }

    public NodeAddress getCurrentLeader() {
        return currentLeader;
    }

    public void setCurrentLeader(NodeAddress currentLeader) {
        assertOnRaftThread();
        this.currentLeader = currentLeader;
    }

    public int quorumSize() {
        return (peers.size() + 1) / 2 + 1;
    }

    // ---------------------------------------------------------------- applying

    public void setCommitIndex(long commitIndex) {
        assertOnRaftThread();
        this.commitIndex = commitIndex;
    }

    public void applyCommittedEntries() {
        assertOnRaftThread();
        while (lastApplied < commitIndex) {
            LogEntry entry = raftLog.getEntry(lastApplied + 1);
            if (entry == null) {
                log.error("Committed entry {} is missing from the log of {}", lastApplied + 1, localAddress.asString());
                return;
            }
            lastApplied++;

            CommandOutcome outcome = CommandOutcome.success(null);
            if (entry.type() == LogEntryType.USER) {
                String requestId = entry.requestId();
                CommandOutcome known = requestId != null ? processedRequests.get(requestId) : null;
                if (known != null) {
                    log.debug("Entry {} repeats request {}: returning the remembered result", lastApplied, requestId);
                    outcome = known;
                } else {
                    outcome = invokeStateMachine(entry);
                    if (requestId != null) {
                        processedRequests.put(requestId, outcome);
                    }
                }
                if (requestId != null) {
                    uncommittedRequests.remove(requestId, entry.index());
                }
            }

            CompletableFuture<Object> pending = pendingClientRequests.remove(lastApplied);
            if (pending != null) {
                outcome.completeInto(pending);
            }
        }
    }

    // A handler exception is the result of the command: lastApplied advances and every node records the same failure
    private CommandOutcome invokeStateMachine(LogEntry entry) {
        KurinCommandContext previous = KurinCommandContext.bind(
                new KurinCommandContext(entry.index(), entry.term(), entry.leaderTimestampMs(), entry.requestId()));
        try {
            log.debug("Applying entry {} ({})", entry.index(), entry.command().getClass().getSimpleName());
            return CommandOutcome.success(stateMachine.apply(entry.command()));
        } catch (StateMachine.HandlerFailedException e) {
            Throwable cause = e.getCause();
            log.debug("Handler of entry {} failed: {}", entry.index(), cause.toString());
            return CommandOutcome.failure(cause.getClass().getName(), cause.getMessage());
        } catch (RuntimeException e) {
            log.warn("Entry {} could not be applied: {}", entry.index(), e.toString());
            return CommandOutcome.failure(e.getClass().getName(), e.getMessage());
        } finally {
            KurinCommandContext.restore(previous);
        }
    }

    public CommandOutcome processedOutcome(String requestId) {
        assertOnRaftThread();
        return requestId != null ? processedRequests.get(requestId) : null;
    }

    /**
     * @return the log index of a not yet applied entry carrying this request id, or null
     */
    public Long uncommittedIndexOf(String requestId) {
        assertOnRaftThread();
        return requestId != null ? uncommittedRequests.get(requestId) : null;
    }

    public void trackUncommittedRequest(String requestId, long logIndex) {
        assertOnRaftThread();
        if (requestId != null) {
            uncommittedRequests.put(requestId, logIndex);
        }
    }

    /**
     * Rebuilds the request index from the unapplied tail of the log. Called by a new leader, whose log
     * may hold requests accepted by its predecessors.
     */
    public void rebuildUncommittedRequests() {
        assertOnRaftThread();
        uncommittedRequests.clear();
        for (LogEntry entry : raftLog.getEntriesFrom(lastApplied + 1)) {
            if (entry.type() == LogEntryType.USER && entry.requestId() != null) {
                uncommittedRequests.put(entry.requestId(), entry.index());
            }
        }
    }

    public CompletableFuture<Object> clientFutureFor(long logIndex) {
        assertOnRaftThread();
        return pendingClientRequests.computeIfAbsent(logIndex, index -> new CompletableFuture<>());
    }

    public void failPendingClientRequests(Throwable reason) {
        assertOnRaftThread();
        List<CompletableFuture<Object>> futures = new ArrayList<>(pendingClientRequests.values());
        pendingClientRequests.clear();
        futures.forEach(future -> future.completeExceptionally(reason));
    }

    // ---------------------------------------------------------------- timers

    /**
     * (Re)starts the election timer for the current role. A callback of an earlier start is ignored even if
     * it is already queued on the Raft thread, because its epoch no longer matches.
     */
    public void resetElectionTimer() {
        assertOnRaftThread();
        cancelElectionTimer();
        long epoch = electionEpoch;
        NodeRole expectedRole = currentRole;
        long timeoutMs = ThreadLocalRandom.current().nextLong(config.getElectionTimeoutMinMs(), config.getElectionTimeoutMaxMs() + 1);

        this.electionTimeout = timer.newTimeout(t -> executeQuietly(() -> {
            if (epoch == electionEpoch && currentRole == expectedRole) {
                log.debug("Election timeout of {} ms fired on {}", timeoutMs, localAddress.asString());
                expectedRole.onElectionTimeout(this);
            }
        }), timeoutMs, TimeUnit.MILLISECONDS);
    }

    public void cancelElectionTimer() {
        assertOnRaftThread();
        electionEpoch++;
        if (electionTimeout != null) {
            electionTimeout.cancel();
            electionTimeout = null;
        }
    }

    public void startHeartbeatTimer() {
        assertOnRaftThread();
        cancelHeartbeatTimer();
        scheduleHeartbeat(heartbeatEpoch, currentRole);
    }

    private void scheduleHeartbeat(long epoch, NodeRole expectedRole) {
        this.heartbeatTimeout = timer.newTimeout(t -> executeQuietly(() -> {
            if (epoch == heartbeatEpoch && currentRole == expectedRole) {
                expectedRole.onHeartbeatTick(this);
                if (epoch == heartbeatEpoch && currentRole == expectedRole) {
                    scheduleHeartbeat(epoch, expectedRole);
                }
            }
        }), config.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);
    }

    public void cancelHeartbeatTimer() {
        assertOnRaftThread();
        heartbeatEpoch++;
        if (heartbeatTimeout != null) {
            heartbeatTimeout.cancel();
            heartbeatTimeout = null;
        }
    }

    // Timer callbacks may fire while the node shuts down
    private void executeQuietly(Runnable task) {
        try {
            execute(task);
        } catch (KurinNotStartedException e) {
            log.debug("Timer callback ignored: {}", e.getMessage());
        }
    }

    /**
     * Completes the future on the Raft thread, or drops the completion if the node is shut down.
     */
    public void onRaftThread(Runnable task) {
        executeQuietly(task);
    }

    // ---------------------------------------------------------------- accessors

    public RaftConfig getConfig() {
        return config;
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

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public KryoSerializer getSerializer() {
        return serializer;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public Duration getRpcTimeout() {
        return rpcTimeout;
    }

    public void setTransport(KurinTransport transport) {
        this.transport = transport;
    }

    public KurinTransport getTransport() {
        return transport;
    }

    public void setTimer(Timer timer) {
        this.timer = timer;
    }
}
