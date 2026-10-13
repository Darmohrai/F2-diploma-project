package org.kurin.raft.state;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import org.kurin.api.KurinCommandContext;
import org.kurin.api.KurinInstrumentation;
import org.kurin.api.KurinObserver;
import org.slf4j.MDC;
import org.kurin.api.exception.KurinNotStartedException;
import org.kurin.api.exception.KurinStorageException;
import org.kurin.network.client.KurinTransport;
import org.kurin.network.model.NodeAddress;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.raft.RaftConfig;
import org.kurin.raft.RaftRole;
import org.kurin.raft.RaftStatus;
import org.kurin.raft.log.LogEntry;
import org.kurin.raft.log.LogEntryType;
import org.kurin.raft.log.RaftLog;
import org.kurin.raft.snapshot.Snapshot;
import org.kurin.raft.snapshot.SnapshotStore;
import java.util.function.BooleanSupplier;
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
    private static final int MAX_TASKS_PER_SYNC = 64;

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
    private final SnapshotStore snapshotStore;
    private final BooleanSupplier moreTasksQueued;
    private int tasksSinceSync;

    // The unguarded storage, used only to find out whether it works again
    private final RaftLog rawLog;
    private final PersistentMetadata rawMetadata;
    private final SnapshotStore rawSnapshotStore;
    // The first I/O error since the storage last worked; null while it works
    private Throwable storageFailure;
    private long storageRetryDelayMs;
    private long storageRetryEpoch;

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
    private volatile org.kurin.api.LeadershipToken leadershipToken;
    private long lastLeaderContactNanos;
    private boolean leaderContactKnown;

    public RaftState(RaftConfig config, NodeAddress localAddress, Set<NodeAddress> peers, RaftLog raftLog,
                     StateMachine stateMachine, PersistentMetadata metadata, KryoSerializer serializer,
                     List<RoleChangeListener> roleListeners, ExecutorService raftExecutor, Thread raftThread,
                     SnapshotStore snapshotStore, BooleanSupplier moreTasksQueued) {
        this.moreTasksQueued = moreTasksQueued;
        this.config = config;
        this.localAddress = localAddress;
        this.peers = Set.copyOf(peers);
        this.rawLog = raftLog;
        this.rawMetadata = metadata;
        this.rawSnapshotStore = snapshotStore;
        // Everything below this constructor talks to the storage through the guard
        GuardedStorage guard = new GuardedStorage(this::recordStorageFailure);
        this.raftLog = guard.guard(raftLog);
        this.metadata = guard.guard(metadata);
        this.snapshotStore = guard.guard(snapshotStore);
        this.stateMachine = stateMachine;
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
                } catch (KurinStorageException e) {
                    // Reported once, with its cause, when the node leaves the cluster below
                    log.debug("A task on the Raft thread of {} was cut short: {}", localAddress.asString(), e.getMessage());
                } catch (Throwable t) {
                    // The Raft thread must survive: a dead thread would silently freeze the node
                    log.error("Unexpected error on the Raft thread of {}", localAddress.asString(), t);
                } finally {
                    flushLogIfIdle();
                    leaveClusterIfStorageFailed();
                    publishStatus();
                }
            });
        } catch (RejectedExecutionException e) {
            throw new KurinNotStartedException("Node " + localAddress.asString() + " is shut down");
        }
    }

    // Group commit: entries appended by the tasks of one burst share a single disk flush
    private void flushLogIfIdle() {
        if (storageFailure != null) {
            return;
        }
        if (raftLog.getDurableIndex() >= raftLog.getLastLogIndex()) {
            tasksSinceSync = 0;
            return;
        }
        if (++tasksSinceSync < MAX_TASKS_PER_SYNC && moreTasksQueued.getAsBoolean()) {
            return;
        }
        tasksSinceSync = 0;
        try {
            raftLog.sync();
            if (currentRole != null) {
                currentRole.onLogDurable(this);
            }
        } catch (KurinStorageException e) {
            // A leader that cannot flush would never count itself towards a commit, and a follower would
            // acknowledge nothing: the node is of no use to the cluster and says so instead of going quiet
            log.debug("The Raft log of {} could not be flushed: {}", localAddress.asString(), e.getMessage());
        } catch (Throwable t) {
            log.error("Unexpected error after flushing the Raft log of {}", localAddress.asString(), t);
        }
    }

    // ---------------------------------------------------------------- storage failure

    // Called by the storage guard, in the middle of whatever operation hit the error
    private KurinStorageException recordStorageFailure(java.io.UncheckedIOException cause) {
        assertOnRaftThread();
        if (storageFailure == null) {
            storageFailure = cause;
        }
        return new KurinStorageException("Node " + localAddress.asString() + " cannot write its durable state: "
                + cause.getMessage(), cause);
    }

    /**
     * @return the exception for a request that reaches this node while its storage is failed
     */
    public KurinStorageException storageUnavailable() {
        Throwable cause = storageFailure;
        return new KurinStorageException("Node " + localAddress.asString() + " is out of service until its storage works again"
                + (cause != null ? ": " + cause.getMessage() : ""));
    }

    public boolean storageFailed() {
        return storageFailure != null;
    }

    /**
     * Runs at the end of every task. The role is changed here and not where the error occurred, because that
     * place is in the middle of a handler whose role would be pulled from under it.
     */
    private void leaveClusterIfStorageFailed() {
        if (storageFailure == null || currentRole == null || currentRole instanceof org.kurin.raft.state.roles.StorageFailedRole) {
            return;
        }
        log.error("Node {} cannot write its durable state and leaves the cluster until the storage works again. "
                + "It will not vote, accept entries or serve clients.", localAddress.asString(), storageFailure);
        try {
            failPendingClientRequests(storageUnavailable());
            currentLeader = null;
            transitionTo(new org.kurin.raft.state.roles.StorageFailedRole());
            Throwable cause = storageFailure;
            observe(observer -> observer.onStorageFailure(cause));
            storageRetryDelayMs = config.getStorageRetryMinMs();
            scheduleStorageRetry();
        } catch (Throwable t) {
            log.error("Unexpected error while node {} was leaving the cluster", localAddress.asString(), t);
        }
    }

    private void scheduleStorageRetry() {
        long epoch = ++storageRetryEpoch;
        long delayMs = storageRetryDelayMs;
        timer.newTimeout(t -> executeQuietly(() -> {
            if (epoch == storageRetryEpoch && currentRole instanceof org.kurin.raft.state.roles.StorageFailedRole) {
                retryStorage();
            }
        }), delayMs, TimeUnit.MILLISECONDS);
    }

    private void retryStorage() {
        try {
            // Whatever was appended but not flushed is written again; a flush alone would not bring it back
            rawLog.recover();
            rawMetadata.save(rawMetadata.getCurrentTerm(), rawMetadata.getVotedFor());
            rawSnapshotStore.verifyWritable();
        } catch (RuntimeException e) {
            storageRetryDelayMs = Math.min(storageRetryDelayMs * 2, config.getStorageRetryMaxMs());
            log.warn("The storage of node {} still fails: {}. Next attempt in {} ms.", localAddress.asString(),
                    e.getMessage(), storageRetryDelayMs);
            scheduleStorageRetry();
            return;
        }
        storageFailure = null;
        log.info("The storage of node {} works again. The node returns to the cluster as a follower.", localAddress.asString());
        observe(KurinObserver::onStorageRecovered);
        transitionTo(new FollowerRole());
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
                commitIndex, lastApplied, raftLog.getLastLogIndex(), lease, localAddress.asString(),
                storageFailure != null ? String.valueOf(storageFailure.getMessage()) : null);
        RaftStatus previous = status;
        if (!fresh.equals(previous)) {
            status = fresh;
            if (fresh.role() != previous.role() || fresh.term() != previous.term() || mdcUnset) {
                // Visible in every log line written from the Raft thread
                MDC.put("kurin.node", fresh.nodeId());
                MDC.put("kurin.term", Long.toString(fresh.term()));
                MDC.put("kurin.role", fresh.role().name());
                mdcUnset = false;
            }
            if (fresh.leaderId() == null && previous.leaderId() != null) {
                leaderUnknownSinceNanos = System.nanoTime();
            }
            if (fresh.leaderId() != null && !fresh.leaderId().equals(previous.leaderId())) {
                observe(observer -> observer.onLeaderChange(fresh.leaderId()));
            }
        }
        logEntries = raftLog.getLastLogIndex() - raftLog.getBaseIndex();
        if (currentRole != null) {
            peerContactNanos = currentRole.peerContactNanos(this);
        }
    }

    // ---------------------------------------------------------------- observability

    private boolean mdcUnset = true;
    private volatile long logEntries;
    private volatile long leaderUnknownSinceNanos = System.nanoTime();
    private volatile Map<String, Long> peerContactNanos = Map.of();
    private List<KurinObserver> observers = List.of();
    private KurinInstrumentation instrumentation = KurinInstrumentation.NOOP;
    // Trace context of requests waiting in the log on this leader; kept in memory only, never replicated
    private final Map<Long, Map<String, String>> traceHeaders = new HashMap<>();

    public void setObservability(List<KurinObserver> observers, KurinInstrumentation instrumentation) {
        this.observers = List.copyOf(observers);
        this.instrumentation = instrumentation;
    }

    public void observe(java.util.function.Consumer<KurinObserver> event) {
        for (KurinObserver observer : observers) {
            try {
                event.accept(observer);
            } catch (RuntimeException e) {
                log.warn("Observer {} failed", observer.getClass().getName(), e);
            }
        }
    }

    public KurinInstrumentation instrumentation() {
        return instrumentation;
    }

    public void rememberTraceHeaders(long logIndex, Map<String, String> headers) {
        assertOnRaftThread();
        if (headers != null && !headers.isEmpty()) {
            traceHeaders.put(logIndex, headers);
        }
    }

    /**
     * @return number of entries currently held in the log; safe to call from any thread
     */
    public long logEntries() {
        return logEntries;
    }

    /**
     * @return for how long this node has known no leader, in nanoseconds; 0 while it knows one. Any thread.
     */
    public long leaderUnknownForNanos() {
        return status.leaderId() != null ? 0 : System.nanoTime() - leaderUnknownSinceNanos;
    }

    /**
     * @return {@link System#nanoTime()} of the last contact with each peer this node has heard from in its
     * current role: on a leader its followers, on a follower its leader. Any thread.
     */
    public Map<String, Long> peerContactNanos() {
        return peerContactNanos;
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

    // ---------------------------------------------------------------- leader contact and leadership

    /**
     * Records that the legitimate leader of the current term was just heard from.
     */
    public void markLeaderContact() {
        assertOnRaftThread();
        lastLeaderContactNanos = System.nanoTime();
        leaderContactKnown = true;
    }

    /**
     * @return true if a leader was heard from within the minimum election timeout. While that holds, this node
     * does not vote for anyone else, which is what makes leader leases safe.
     */
    /**
     * @return {@link System#nanoTime()} of the last message from the leader, or null if none was received yet
     */
    public Long lastLeaderContactNanos() {
        assertOnRaftThread();
        return leaderContactKnown ? lastLeaderContactNanos : null;
    }

    public boolean heardFromLeaderRecently() {
        assertOnRaftThread();
        return leaderContactKnown
                && System.nanoTime() - lastLeaderContactNanos < java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(config.getElectionTimeoutMinMs());
    }

    /**
     * Publishes or withdraws (null) the proof of established leadership of this node.
     */
    public void setLeadershipToken(org.kurin.api.LeadershipToken token) {
        assertOnRaftThread();
        this.leadershipToken = token;
    }

    /**
     * @return the token of the leadership established by this node, or null; safe to call from any thread.
     * The caller must also check that the lease in {@link #status()} is still valid.
     */
    public org.kurin.api.LeadershipToken leadershipToken() {
        return leadershipToken;
    }

    /**
     * Runs a query against the local state machine.
     */
    public CompletableFuture<Object> executeQuery(Object query) {
        assertOnRaftThread();
        try {
            return CompletableFuture.completedFuture(stateMachine.executeQuery(query));
        } catch (StateMachine.HandlerFailedException e) {
            Throwable cause = e.getCause();
            return CompletableFuture.failedFuture(new org.kurin.api.exception.KurinCommandFailedException(cause.getClass().getName(), cause.getMessage()));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(new org.kurin.api.exception.KurinCommandFailedException(e.getClass().getName(), e.getMessage()));
        }
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
                // A caller that sees its command complete must also see it in status()
                publishStatus();
                outcome.completeInto(pending);
            }
        }
        if (lastApplied - raftLog.getBaseIndex() >= config.getSnapshotThresholdEntries()) {
            takeSnapshot();
        }
    }

    // ---------------------------------------------------------------- snapshots

    /**
     * Captures the state machine at {@code lastApplied}, stores it and drops the log entries it covers.
     */
    public void takeSnapshot() {
        assertOnRaftThread();
        long index = lastApplied;
        if (index <= raftLog.getBaseIndex()) {
            return;
        }
        long startedNanos = System.nanoTime();
        long term = raftLog.getEntry(index).term();
        Snapshot snapshot = new Snapshot(index, term, stateMachine.takeSnapshot(), new LinkedHashMap<>(processedRequests));
        // The snapshot is durable before the entries it replaces are dropped
        snapshotStore.save(snapshot);
        raftLog.compact(index, term);
        traceHeaders.keySet().removeIf(logIndex -> logIndex <= index);
        long snapshotNanos = System.nanoTime() - startedNanos;
        observe(observer -> observer.onSnapshot(snapshotNanos, snapshot.data().length));
        log.info("Node {} took a snapshot at index {} ({} bytes of state, {} ms)", localAddress.asString(), index,
                snapshot.data().length, (System.nanoTime() - startedNanos) / 1_000_000);
    }

    /**
     * @return the latest stored snapshot, or null
     */
    public Snapshot latestSnapshot() {
        assertOnRaftThread();
        return snapshotStore.load();
    }

    /**
     * Replaces the local state with a snapshot received from the leader.
     */
    public void installSnapshot(Snapshot snapshot) {
        assertOnRaftThread();
        snapshotStore.save(snapshot);

        LogEntry local = raftLog.getEntry(snapshot.lastIncludedIndex());
        if (local != null && local.term() == snapshot.lastIncludedTerm()) {
            // The log agrees with the snapshot: entries after it are still valid
            raftLog.compact(snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm());
        } else {
            raftLog.reset(snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm());
        }
        loadSnapshotIntoStateMachine(snapshot);
        log.info("Node {} installed a snapshot up to index {}", localAddress.asString(), snapshot.lastIncludedIndex());
    }

    /**
     * Brings the node back to its last durable state. Called once, before the node takes a role.
     * Entries after the snapshot stay unapplied until a leader confirms how far the log is committed.
     */
    public void restoreFromStorage() {
        assertOnRaftThread();
        Snapshot snapshot = snapshotStore.load();
        if (snapshot == null) {
            if (raftLog.getBaseIndex() > 0) {
                throw new IllegalStateException("The Raft log of " + localAddress.asString() + " starts after index "
                        + raftLog.getBaseIndex() + " but the snapshot covering the earlier entries is missing");
            }
            return;
        }
        if (raftLog.getBaseIndex() > snapshot.lastIncludedIndex()) {
            throw new IllegalStateException("The Raft log of " + localAddress.asString() + " starts after index "
                    + raftLog.getBaseIndex() + " but the stored snapshot ends at " + snapshot.lastIncludedIndex());
        }
        if (raftLog.getBaseIndex() < snapshot.lastIncludedIndex()) {
            // The node stopped between saving the snapshot and compacting the log
            LogEntry local = raftLog.getEntry(snapshot.lastIncludedIndex());
            if (local != null && local.term() == snapshot.lastIncludedTerm()) {
                raftLog.compact(snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm());
            } else {
                raftLog.reset(snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm());
            }
        }
        loadSnapshotIntoStateMachine(snapshot);
        log.info("Node {} restored its state from the snapshot at index {}; {} later entries await commit confirmation",
                localAddress.asString(), snapshot.lastIncludedIndex(), raftLog.getLastLogIndex() - snapshot.lastIncludedIndex());
    }

    private void loadSnapshotIntoStateMachine(Snapshot snapshot) {
        stateMachine.installSnapshot(snapshot.data());
        commitIndex = snapshot.lastIncludedIndex();
        lastApplied = snapshot.lastIncludedIndex();
        processedRequests.clear();
        processedRequests.putAll(snapshot.dedupTable());
        uncommittedRequests.clear();
    }

    // A handler exception is the result of the command: lastApplied advances and every node records the same failure
    private CommandOutcome invokeStateMachine(LogEntry entry) {
        String type = entry.command().getClass().getSimpleName();
        long startedNanos = System.nanoTime();
        // On the leader the span continues the trace of the caller; elsewhere it stands alone
        KurinInstrumentation.Span span = startSpan("kurin.raft.apply", Map.of("type", type), traceHeaders.remove(entry.index()));
        Throwable failure = null;
        KurinCommandContext previous = KurinCommandContext.bind(
                new KurinCommandContext(entry.index(), entry.term(), entry.leaderTimestampMs(), entry.requestId()));
        try {
            log.debug("Applying entry {} ({})", entry.index(), type);
            return CommandOutcome.success(stateMachine.apply(entry.command()));
        } catch (StateMachine.HandlerFailedException e) {
            Throwable cause = e.getCause();
            failure = cause;
            log.debug("Handler of entry {} failed: {}", entry.index(), cause.toString());
            return CommandOutcome.failure(cause.getClass().getName(), cause.getMessage());
        } catch (RuntimeException e) {
            failure = e;
            log.warn("Entry {} could not be applied: {}", entry.index(), e.toString());
            return CommandOutcome.failure(e.getClass().getName(), e.getMessage());
        } finally {
            KurinCommandContext.restore(previous);
            endSpan(span, failure);
            long durationNanos = System.nanoTime() - startedNanos;
            observe(observer -> observer.onApply(type, durationNanos));
        }
    }

    /**
     * Starts a span, shielding the caller from a failing tracing system.
     */
    public KurinInstrumentation.Span startSpan(String name, Map<String, String> tags, Map<String, String> parentHeaders) {
        try {
            KurinInstrumentation.Span span = instrumentation.start(name, tags, parentHeaders);
            return span != null ? span : KurinInstrumentation.Span.NOOP;
        } catch (RuntimeException e) {
            log.debug("Tracing of {} failed to start: {}", name, e.toString());
            return KurinInstrumentation.Span.NOOP;
        }
    }

    public void endSpan(KurinInstrumentation.Span span, Throwable error) {
        try {
            span.end(error);
        } catch (RuntimeException e) {
            log.debug("Tracing failed to end a span: {}", e.toString());
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
