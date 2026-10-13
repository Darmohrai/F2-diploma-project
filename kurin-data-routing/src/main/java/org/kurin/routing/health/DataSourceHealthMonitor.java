package org.kurin.routing.health;

import org.kurin.routing.DataSourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Knows which database nodes are usable. A node is checked periodically with {@code Connection.isValid};
 * in addition, a failure to obtain a connection reported by the router marks the node down at once, without
 * waiting for the next check. A node that was down counts as up again only after several successful checks
 * in a row, so a flapping node does not flap the routing.
 */
public class DataSourceHealthMonitor implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DataSourceHealthMonitor.class);

    public record NodeId(String shard, DataSourceType role) {
        @Override
        public String toString() {
            return shard + "/" + role;
        }
    }

    /**
     * Told when a node changes between usable and unusable. Runs on the monitor thread or on the thread that
     * reported a failure; it must not block.
     */
    public interface Listener {
        void onStateChange(NodeId node, boolean up, String reason);
    }

    private static final class Node {
        final NodeId id;
        final DataSource dataSource;
        volatile boolean up = true;
        volatile String lastFailure;
        int consecutiveSuccesses;

        Node(NodeId id, DataSource dataSource) {
            this.id = id;
            this.dataSource = dataSource;
        }
    }

    private final Map<NodeId, Node> nodes = new ConcurrentHashMap<>();
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();
    private final Duration interval;
    private final int validationTimeoutSeconds;
    private final int recoveryThreshold;
    private ScheduledExecutorService scheduler;

    /**
     * @param interval          time between checks of every node
     * @param validationTimeout longest time one check may take
     * @param recoveryThreshold successful checks in a row after which a node that was down is up again
     */
    public DataSourceHealthMonitor(Duration interval, Duration validationTimeout, int recoveryThreshold) {
        this.interval = interval;
        this.validationTimeoutSeconds = (int) Math.max(1, (validationTimeout.toMillis() + 999) / 1000);
        this.recoveryThreshold = Math.max(1, recoveryThreshold);
    }

    public void register(String shard, DataSourceType role, DataSource dataSource) {
        NodeId id = new NodeId(shard, role);
        nodes.put(id, new Node(id, dataSource));
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
     * Called by the router when it could not get a connection from the node.
     */
    public void reportFailure(String shard, DataSourceType role, Throwable error) {
        Node node = nodes.get(new NodeId(shard, role));
        if (node != null) {
            markDown(node, String.valueOf(error));
        }
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
        boolean valid;
        String failure = null;
        try (Connection connection = node.dataSource.getConnection()) {
            valid = connection.isValid(validationTimeoutSeconds);
            if (!valid) {
                failure = "connection is not valid";
            }
        } catch (Exception e) {
            valid = false;
            failure = e.toString();
        }

        if (!valid) {
            markDown(node, failure);
            return;
        }
        synchronized (node) {
            if (node.up) {
                return;
            }
            if (++node.consecutiveSuccesses < recoveryThreshold) {
                return;
            }
            node.up = true;
            node.lastFailure = null;
        }
        log.info("Database node {} is available again", node.id);
        notifyListeners(node.id, true, "recovered");
    }

    private void markDown(Node node, String reason) {
        synchronized (node) {
            node.consecutiveSuccesses = 0;
            node.lastFailure = reason;
            if (!node.up) {
                return;
            }
            node.up = false;
        }
        log.warn("Database node {} is unavailable: {}", node.id, reason);
        notifyListeners(node.id, false, reason);
    }

    private void notifyListeners(NodeId id, boolean up, String reason) {
        for (Listener listener : listeners) {
            try {
                listener.onStateChange(id, up, reason);
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
    }
}
