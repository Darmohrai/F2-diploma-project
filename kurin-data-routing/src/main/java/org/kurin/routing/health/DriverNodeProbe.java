package org.kurin.routing.health;

import org.kurin.routing.health.ConnectionFailureClassifier.FailureKind;
import org.kurin.routing.health.DataSourceHealthMonitor.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

/**
 * Checks a node over a connection that belongs to the check alone, opened through the JDBC driver with the
 * URL and credentials of the node. It never asks the pool of the application for a connection, so a pool that
 * is exhausted under load neither delays the check nor fails it.
 * <p>
 * <b>One kept connection per node, not one per check.</b> The connection is opened at the first check, asked
 * {@code isValid} at every further one and replaced when it stopped working. The alternative, a new connection
 * for every check, was rejected: it costs a login per node every few seconds, and, worse, it fails exactly when
 * the database is at its connection limit, which is the overloaded but healthy node this probe exists to tell
 * from a dead one. A kept connection holds its slot. A kept connection can go stale without the node being at
 * fault (a restart of the database, a firewall that drops idle connections); a check that finds it so opens a
 * new one before it decides, so a stale connection alone never counts as a failed check.
 * <p>
 * <b>Time limits without driver-specific settings.</b> JDBC has no portable connect timeout, so every check
 * runs on a thread of its own per node and is given up after the timeout; its connection is then aborted.
 * <p>
 * <b>When the node cannot be checked this way.</b> The parameters are read from the data source
 * ({@link ConnectionParameters#of}). Without them, or without a registered driver for the URL, the node is
 * checked through the pool, as {@link PoolNodeProbe} does, and a warning says so once. The same happens when the
 * connection of the check has never worked although the pool does: then it is this way of connecting that is
 * wrong (properties the pool sets and the URL lacks, credentials obtained elsewhere), not the node.
 */
public class DriverNodeProbe implements NodeProbe {
    private static final Logger log = LoggerFactory.getLogger(DriverNodeProbe.class);

    private final BiFunction<NodeId, DataSource, Optional<ConnectionParameters>> parameters;
    private final NodeProbe throughPool = new PoolNodeProbe();
    private final Map<NodeId, ServiceConnection> connections = new ConcurrentHashMap<>();

    /**
     * Takes the connection parameters of every node from its data source.
     */
    public DriverNodeProbe() {
        this((node, dataSource) -> ConnectionParameters.of(dataSource));
    }

    /**
     * @param parameters how to connect to a node; empty for a node that is to be checked through its pool
     */
    public DriverNodeProbe(BiFunction<NodeId, DataSource, Optional<ConnectionParameters>> parameters) {
        this.parameters = parameters;
    }

    @Override
    public void register(NodeId node, DataSource dataSource) {
        Optional<ConnectionParameters> known = parameters.apply(node, dataSource);
        if (known.isEmpty()) {
            log.warn("Database node {} is checked through the connection pool of the application: its data source ({}) "
                    + "does not tell how to connect to it. Such a check competes with the application for connections "
                    + "and learns nothing while the pool is exhausted. Supply a NodeProbe, or a data source that exposes "
                    + "its JDBC URL.", node, dataSource.getClass().getName());
            return;
        }
        try {
            DriverManager.getDriver(known.get().url());
        } catch (SQLException e) {
            log.warn("Database node {} is checked through the connection pool of the application: no JDBC driver is "
                    + "registered for its URL ({}). Such a check competes with the application for connections.",
                    node, e.getMessage());
            return;
        }
        connections.put(node, new ServiceConnection(node, known.get()));
        log.debug("Database node {} is checked over a connection of its own", node);
    }

    @Override
    public void check(NodeId node, DataSource dataSource, Duration timeout) throws Exception {
        ServiceConnection connection = connections.get(node);
        if (connection == null) {
            throughPool.check(node, dataSource, timeout);
            return;
        }
        try {
            connection.check(timeout);
        } catch (Exception failure) {
            if (connection.hasWorked()) {
                throw failure;
            }
            // It has never worked. Either the node is away, or this way of connecting to it is wrong.
            try {
                throughPool.check(node, dataSource, timeout);
            } catch (Exception poolFailure) {
                // A pool that is merely busy settles nothing: let the monitor see that, not a verdict on the node
                throw ConnectionFailureClassifier.classify(poolFailure) == FailureKind.POOL_EXHAUSTED ? poolFailure : failure;
            }
            if (connections.remove(node, connection)) {
                connection.close();
                log.warn("Database node {} answers through the connection pool of the application but not over a "
                        + "connection of the health check ({}). From now on it is checked through the pool, which "
                        + "competes with the application for connections. If the pool connects with properties that "
                        + "are not part of the JDBC URL, supply a NodeProbe that knows them.", node, failure.toString());
            }
        }
    }

    @Override
    public void close() {
        connections.values().forEach(ServiceConnection::close);
        connections.clear();
    }

    /**
     * The connection kept for checking one node, and the thread its checks run on.
     */
    private static final class ServiceConnection {
        private final NodeId node;
        private final ConnectionParameters parameters;
        private final ExecutorService thread;
        private volatile Connection connection;
        private volatile boolean worked;

        ServiceConnection(NodeId node, ConnectionParameters parameters) {
            this.node = node;
            this.parameters = parameters;
            this.thread = Executors.newSingleThreadExecutor(task -> {
                Thread created = new Thread(task, "kurin-routing-probe-" + node.shard() + "-" + node.role());
                created.setDaemon(true);
                return created;
            });
        }

        boolean hasWorked() {
            return worked;
        }

        void check(Duration timeout) throws Exception {
            Future<?> check = thread.submit(() -> {
                validate(PoolNodeProbe.seconds(timeout));
                return null;
            });
            try {
                check.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                worked = true;
            } catch (ExecutionException e) {
                throw e.getCause() instanceof Exception cause ? cause : e;
            } catch (TimeoutException e) {
                check.cancel(true);
                discard();
                throw new SQLTimeoutException("Database node " + node + " did not answer the health check within "
                        + timeout.toMillis() + " ms");
            }
        }

        // Runs on the thread of this connection
        private void validate(int timeoutSeconds) throws SQLException {
            Connection kept = connection;
            if (kept != null) {
                if (isValid(kept, timeoutSeconds)) {
                    return;
                }
                // Stale, or the node is away: a fresh connection tells which
                discard();
            }
            Connection fresh = open();
            if (!isValid(fresh, timeoutSeconds)) {
                closeQuietly(fresh);
                throw new SQLNonTransientConnectionException("A new connection to " + node + " is not valid");
            }
            connection = fresh;
        }

        private Connection open() throws SQLException {
            Properties properties = new Properties();
            if (parameters.username() != null) {
                properties.setProperty("user", parameters.username());
            }
            if (parameters.password() != null) {
                properties.setProperty("password", parameters.password());
            }
            return DriverManager.getConnection(parameters.url(), properties);
        }

        private static boolean isValid(Connection connection, int timeoutSeconds) {
            try {
                return connection.isValid(timeoutSeconds);
            } catch (SQLException e) {
                log.debug("A health check connection could not be validated: {}", e.toString());
                return false;
            }
        }

        private void discard() {
            Connection kept = connection;
            connection = null;
            if (kept != null) {
                closeQuietly(kept);
            }
        }

        // The connection is of no further use; a failure to close it changes nothing and is worth a debug line
        private static void closeQuietly(Connection connection) {
            try {
                // abort does not wait for a call that hangs on a dead socket, close would
                connection.abort(Runnable::run);
            } catch (SQLException | RuntimeException | AbstractMethodError e) {
                log.debug("A health check connection could not be aborted: {}", e.toString());
            }
            try {
                // Not every driver ends the session on abort
                if (!connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException | RuntimeException e) {
                log.debug("A health check connection could not be closed: {}", e.toString());
            }
        }

        void close() {
            thread.shutdownNow();
            discard();
        }
    }
}
