package org.kurin.routing.health;

import org.kurin.routing.DataSourceType;
import org.kurin.routing.health.ConnectionFailureClassifier.FailureKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Knows which database nodes are usable. Two things can take a node out of use:
 * <ul>
 * <li>the periodic check, made by a {@link NodeProbe}, failed several times in a row
 * ({@code failureThreshold}); a single failed check is not enough, so one lost packet does not move traffic;</li>
 * <li>the router could not get a connection for a request, for a reason that lies with the node: it is then
 * marked down at once, without waiting for the checks.</li>
 * </ul>
 * A pool that had no free connection in time is neither. The node behind an exhausted pool works, and marking
 * it down would send its load to the nodes that are left. Such a failure, from a request or from a check that
 * goes through the pool, is reported to the listeners and leaves the state of the node as it is; see
 * {@link ConnectionFailureClassifier}.
 * <p>
 * A node that was down counts as up again only after several successful checks in a row
 * ({@code recoveryThreshold}), so a flapping node does not flap the routing.
 */
public class DataSourceHealthMonitor implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DataSourceHealthMonitor.class);

    /**
     * Failed checks in a row after which a node is taken out of use, unless told otherwise.
     */
    public static final int DEFAULT_FAILURE_THRESHOLD = 2;

    public record NodeId(String shard, DataSourceType role) {
        @Override
        public String toString() {
            return shard + "/" + role;
        }
    }

    /**
     * Told what the monitor finds out. The methods run on the monitor thread or on the thread that reported a
     * failure; they must not block.
     */
    public interface Listener {
        /**
         * A node changed between usable and unusable.
         */
        void onStateChange(NodeId node, boolean up, String reason);

        /**
         * A connection could not be obtained because the pool of the node had none free in time. The node stays
         * in the state it was in.
         */
        default void onPoolExhausted(NodeId node, Throwable error) {
        }
    }

    private static final class Node {
        final NodeId id;
        final DataSource dataSource;
        volatile boolean up = true;
        volatile String lastFailure;
        volatile FailureKind lastFailureKind;
        int consecutiveSuccesses;
        int consecutiveFailures;

        Node(NodeId id, DataSource dataSource) {
            this.id = id;
            this.dataSource = dataSource;
        }
    }

    private final Map<NodeId, Node> nodes = new ConcurrentHashMap<>();
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();
    private final Duration interval;
    private final Duration validationTimeout;
    private final int failureThreshold;
    private final int recoveryThreshold;
    private final NodeProbe probe;
    private ScheduledExecutorService scheduler;

    /**
     * A monitor with {@link #DEFAULT_FAILURE_THRESHOLD} and a {@link DriverNodeProbe}.
     *
     * @param interval          time between checks of every node
     * @param validationTimeout longest time one check may take
     * @param recoveryThreshold successful checks in a row after which a node that was down is up again
     */
    public DataSourceHealthMonitor(Duration interval, Duration validationTimeout, int recoveryThreshold) {
        this(interval, validationTimeout, DEFAULT_FAILURE_THRESHOLD, recoveryThreshold, new DriverNodeProbe());
    }

    /**
     * @param interval          time between checks of every node
     * @param validationTimeout longest time one check may take
     * @param failureThreshold  failed checks in a row after which a node that was up is down
     * @param recoveryThreshold successful checks in a row after which a node that was down is up again
     * @param probe             how a node is checked
     */
    public DataSourceHealthMonitor(Duration interval, Duration validationTimeout, int failureThreshold, int recoveryThreshold,
                                   NodeProbe probe) {
        this.interval = interval;
        this.validationTimeout = validationTimeout;
        this.failureThreshold = Math.max(1, failureThreshold);
        this.recoveryThreshold = Math.max(1, recoveryThreshold);
        this.probe = probe;
    }

    public void register(String shard, DataSourceType role, DataSource dataSource) {
        NodeId id = new NodeId(shard, role);
        nodes.put(id, new Node(id, dataSource));
        probe.register(id, dataSource);
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public synchronized void start() {
        if (scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "kurin-routing-health");
            thread.setDaemon(true);
            return thread;
        });
        long intervalMs = Math.max(1, interval.toMillis());
        scheduler.scheduleWithFixedDelay(this::checkAll, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * @return true if the node is believed usable; nodes that were never registered count as usable
     */
    public boolean isUp(String shard, DataSourceType role) {
        Node node = nodes.get(new NodeId(shard, role));
        return node == null || node.up;
    }

    public Map<NodeId, Boolean> states() {
        Map<NodeId, Boolean> states = new java.util.TreeMap<>(java.util.Comparator.comparing(NodeId::toString));
        nodes.forEach((id, node) -> states.put(id, node.up));
        return states;
    }

    /**
     * @return why the node is down, as reported by the check or the router that found it so; null while it is up
     */
    public String failureOf(String shard, DataSourceType role) {
        Node node = nodes.get(new NodeId(shard, role));
        return node != null && !node.up ? node.lastFailure : null;
    }

    /**
     * @return what kind of failure took the node down; null while it is up
     */
    public FailureKind failureKindOf(String shard, DataSourceType role) {
        Node node = nodes.get(new NodeId(shard, role));
        return node != null && !node.up ? node.lastFailureKind : null;
    }

    /**
     * Called by the router when it could not get a connection from the node. Same as
     * {@link #reportConnectionFailure}, for callers that do not need the verdict.
     */
    public void reportFailure(String shard, DataSourceType role, Throwable error) {
        reportConnectionFailure(shard, role, error);
    }

    /**
     * Called by the router when it could not get a connection from the node for a request. A failure that lies
     * with the node marks it down at once. A pool without a free connection does not: the listeners are told,
     * and the node stays as it is.
     *
     * @return what the failure was taken for, so that the caller can treat a busy pool differently from a node
     * that is away
     */
    public FailureKind reportConnectionFailure(String shard, DataSourceType role, Throwable error) {
        FailureKind kind = ConnectionFailureClassifier.classify(error);
        Node node = nodes.get(new NodeId(shard, role));
        if (node == null) {
            return kind;
        }
        if (kind == FailureKind.POOL_EXHAUSTED) {
            log.debug("The pool of database node {} had no free connection: {}. The node is not marked down.", node.id, String.valueOf(error));
            notifyPoolExhausted(node.id, error);
        } else {
            markDown(node, String.valueOf(error), kind);
        }
        return kind;
    }

    /**
     * Checks every node now. Normally run by the scheduler; public so that tests need not wait for it.
     */
    public void checkAll() {
        for (Node node : nodes.values()) {
            try {
                check(node);
            } catch (RuntimeException e) {
                log.warn("Health check of {} failed unexpectedly", node.id, e);
            }
        }
    }

    private void check(Node node) {
        try {
            probe.check(node.id, node.dataSource, validationTimeout);
        } catch (Exception e) {
            FailureKind kind = ConnectionFailureClassifier.classify(e);
            if (kind == FailureKind.POOL_EXHAUSTED) {
                // The check went through the pool and got no connection: nothing was learned about the node
                log.debug("Health check of {} found its pool without a free connection: {}", node.id, e.toString());
                notifyPoolExhausted(node.id, e);
            } else {
                failedCheck(node, e.toString(), kind);
            }
            return;
        }

        synchronized (node) {
            node.consecutiveFailures = 0;
            if (node.up) {
                return;
            }
            if (++node.consecutiveSuccesses < recoveryThreshold) {
                return;
            }
            node.up = true;
            node.lastFailure = null;
            node.lastFailureKind = null;
        }
        log.info("Database node {} is available again", node.id);
        notifyStateChange(node.id, true, "recovered");
    }

    private void failedCheck(Node node, String reason, FailureKind kind) {
        synchronized (node) {
            if (node.up && ++node.consecutiveFailures < failureThreshold) {
                node.consecutiveSuccesses = 0;
                log.debug("Health check of {} failed ({} of {} in a row before it is marked down): {}",
                        node.id, node.consecutiveFailures, failureThreshold, reason);
                return;
            }
        }
        markDown(node, reason, kind);
    }

    private void markDown(Node node, String reason, FailureKind kind) {
        synchronized (node) {
            node.consecutiveSuccesses = 0;
            node.consecutiveFailures = 0;
            node.lastFailure = reason;
            node.lastFailureKind = kind;
            if (!node.up) {
                return;
            }
            node.up = false;
        }
        log.warn("Database node {} is unavailable ({}): {}", node.id, kind, reason);
        notifyStateChange(node.id, false, reason);
    }

    private void notifyStateChange(NodeId id, boolean up, String reason) {
        for (Listener listener : listeners) {
            try {
                listener.onStateChange(id, up, reason);
            } catch (RuntimeException e) {
                log.warn("Health listener {} failed", listener.getClass().getName(), e);
            }
        }
    }

    private void notifyPoolExhausted(NodeId id, Throwable error) {
        for (Listener listener : listeners) {
            try {
                listener.onPoolExhausted(id, error);
            } catch (RuntimeException e) {
                log.warn("Health listener {} failed", listener.getClass().getName(), e);
            }
        }
    }

    @Override
    public synchronized void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        probe.close();
    }
}
