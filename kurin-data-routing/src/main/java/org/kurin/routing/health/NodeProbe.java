package org.kurin.routing.health;

import org.kurin.routing.health.DataSourceHealthMonitor.NodeId;

import javax.sql.DataSource;
import java.time.Duration;

/**
 * Finds out whether a database node answers. The health monitor calls it for every node at every check.
 * <p>
 * Two implementations exist. {@link DriverNodeProbe}, the default, keeps a connection of its own to the node, so
 * a check never waits for the pool the application works with. {@link PoolNodeProbe} borrows a connection from
 * that pool, which is all that can be done when the monitor is not told how to connect by itself. An
 * application can supply its own, for example one that asks a proxy or an orchestrator instead of the database.
 */
@FunctionalInterface
public interface NodeProbe {

    /**
     * Checks one node. Returning normally means that the node answers.
     *
     * @param node       which node
     * @param dataSource the pool the application uses for that node
     * @param timeout    longest time the check may take
     * @throws Exception if the node does not answer. The monitor classifies it with
     *                   {@link ConnectionFailureClassifier}: a failure that only says that the pool was busy
     *                   leaves the state of the node as it is.
     */
    void check(NodeId node, DataSource dataSource, Duration timeout) throws Exception;

    /**
     * Called once for every node when it is registered, before the first check. A probe may prepare what it needs
     * for the node here and say how it is going to check it.
     */
    default void register(NodeId node, DataSource dataSource) {
    }

    /**
     * Releases whatever the probe holds. Called when the monitor is closed.
     */
    default void close() {
    }
}
