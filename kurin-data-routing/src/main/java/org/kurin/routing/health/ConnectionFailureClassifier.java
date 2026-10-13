package org.kurin.routing.health;

import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Tells why a connection could not be obtained from a pool: because the database node behind the pool cannot be
 * used, or because the pool had no free connection to give. The two look alike to the caller, who gets an
 * {@code SQLException} after waiting either way, and they call for opposite reactions: a node that is away must
 * not be asked again, a pool that is merely busy belongs to a node in perfect health.
 * <p>
 * A failure is taken for an exhausted pool only when it is recognised as one and nothing in it points at the
 * node. Everything that is not recognised counts against the node, so that an unknown way of failing is never
 * mistaken for load.
 */
public final class ConnectionFailureClassifier {
    private static final int MAX_DEPTH = 32;

    public enum FailureKind {
        /**
         * The node was not reached or dropped the connection: SQLState class 08, a
         * {@code SQLNonTransientConnectionException}, a refused or timed out socket, an unknown host. A pool
         * reports the same when it timed out because it could not create a connection.
         */
        CONNECTION_FAILED(true),
        /**
         * The node was reached and refused the credentials: SQLState class 28.
         */
        AUTHENTICATION_FAILED(true),
        /**
         * The pool had no free connection within its timeout, and no attempt to create one had failed: the
         * connections exist and are all in use. This says nothing about the node.
         */
        POOL_EXHAUSTED(false),
        /**
         * None of the above. Counted against the node.
         */
        UNKNOWN(true);

        private final boolean nodeUnavailable;

        FailureKind(boolean nodeUnavailable) {
            this.nodeUnavailable = nodeUnavailable;
        }

        /**
         * @return true if the failure means that the node should not be used until a check finds it working
         */
        public boolean nodeUnavailable() {
            return nodeUnavailable;
        }
    }

    private ConnectionFailureClassifier() {
    }

    /**
     * @param error what {@code DataSource.getConnection()} or a check of a connection threw
     */
    public static FailureKind classify(Throwable error) {
        List<Throwable> chain = chainOf(error);
        // A sign of trouble with the node wins wherever it sits: HikariCP reports a pool that timed out because
        // connections could not be created as a timeout whose cause is the last failed creation
        for (Throwable link : chain) {
            FailureKind node = nodeFailure(link);
            if (node != null) {
                return node;
            }
        }
        for (Throwable link : chain) {
            if (poolTimeout(link)) {
                return FailureKind.POOL_EXHAUSTED;
            }
        }
        return FailureKind.UNKNOWN;
    }

    private static FailureKind nodeFailure(Throwable link) {
        if (link instanceof SQLException sql) {
            String state = sql.getSQLState();
            if (state != null && state.startsWith("28")) {
                return FailureKind.AUTHENTICATION_FAILED;
            }
            if (state != null && state.startsWith("08")) {
                return FailureKind.CONNECTION_FAILED;
            }
            if (link instanceof SQLInvalidAuthorizationSpecException) {
                return FailureKind.AUTHENTICATION_FAILED;
            }
            if (link instanceof SQLNonTransientConnectionException) {
                return FailureKind.CONNECTION_FAILED;
            }
            return null;
        }
        // SocketException covers a refused connection, an unreachable host and a connection reset
        if (link instanceof SocketException || link instanceof UnknownHostException || link instanceof SocketTimeoutException) {
            return FailureKind.CONNECTION_FAILED;
        }
        return null;
    }

    // How the common pools say "no connection became free in time". Compared by name: none of them is a dependency.
    private static boolean poolTimeout(Throwable link) {
        if (link instanceof SQLTransientConnectionException sql) {
            // HikariCP: "Connection is not available, request timed out after ...ms", without a state and
            // without a cause when no creation of a connection had failed
            return sql.getSQLState() == null && sql.getCause() == null;
        }
        if (link instanceof NoSuchElementException && link.getMessage() != null
                && link.getMessage().startsWith("Timeout waiting for idle object")) {
            // Commons Pool, behind Commons DBCP: the cause of "Cannot get a connection, pool error ..."
            return true;
        }
        for (Class<?> type = link.getClass(); type != null; type = type.getSuperclass()) {
            if (type.getName().equals("org.apache.tomcat.jdbc.pool.PoolExhaustedException")) {
                return true;
            }
        }
        return false;
    }

    // The exception, its causes and, for SQL exceptions, the ones chained with setNextException
    private static List<Throwable> chainOf(Throwable error) {
        List<Throwable> chain = new ArrayList<>();
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        collect(error, chain, seen);
        return chain;
    }

    private static void collect(Throwable error, List<Throwable> chain, Set<Throwable> seen) {
        Throwable current = error;
        while (current != null && chain.size() < MAX_DEPTH && seen.add(current)) {
            chain.add(current);
            if (current instanceof SQLException sql && sql.getNextException() != null) {
                collect(sql.getNextException(), chain, seen);
            }
            current = current.getCause();
        }
    }
}
