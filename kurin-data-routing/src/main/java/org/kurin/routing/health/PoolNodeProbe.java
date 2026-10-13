package org.kurin.routing.health;

import org.kurin.routing.health.DataSourceHealthMonitor.NodeId;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLNonTransientConnectionException;
import java.time.Duration;

/**
 * Checks a node through the pool of the application: borrows a connection and asks it whether it is valid.
 * <p>
 * The check competes with the application for connections. When the pool is exhausted it waits as long as the
 * pool lets anyone wait and then fails with the timeout of the pool; the monitor recognises that failure and
 * does not hold it against the node, but it learns nothing from such a check either. Prefer
 * {@link DriverNodeProbe} wherever the connection parameters of the node are known.
 */
public class PoolNodeProbe implements NodeProbe {

    @Override
    public void check(NodeId node, DataSource dataSource, Duration timeout) throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            if (!connection.isValid(seconds(timeout))) {
                throw new SQLNonTransientConnectionException("A connection to " + node + " is not valid");
            }
        }
    }

    // Connection.isValid takes whole seconds, and zero would mean "no limit"
    static int seconds(Duration timeout) {
        return (int) Math.max(1, (timeout.toMillis() + 999) / 1000);
    }
}
