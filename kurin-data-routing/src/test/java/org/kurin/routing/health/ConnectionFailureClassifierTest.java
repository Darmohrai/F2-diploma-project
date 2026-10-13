package org.kurin.routing.health;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.Test;
import org.kurin.routing.health.ConnectionFailureClassifier.FailureKind;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.ServerErrorMessage;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The failures are the real ones wherever a pool or a driver can be made to produce them without a database
 * server: pools that are exhausted, pools and drivers that find nobody at the address. What only a running
 * server can say (wrong password, too many connections) is built the way the PostgreSQL driver builds it, from
 * the error message of the server with its SQLState.
 */
class ConnectionFailureClassifierTest {

    private static FailureKind classify(Throwable error) {
        return ConnectionFailureClassifier.classify(error);
    }

    // ---------------------------------------------------------------- a pool without a free connection

    @Test
    void hikariTimeoutWithAllConnectionsInUseIsAnExhaustedPool() throws Exception {
        try (HikariDataSource pool = Pools.hikari(Pools.memoryDatabase(), 1);
             Connection held = Pools.take(pool)) {

            SQLException timeout = catchThrowableOfType(pool::getConnection, SQLException.class);

            // What HikariCP throws: no state, no cause, because no creation of a connection had failed
            assertThat((Throwable) timeout).isInstanceOf(SQLTransientConnectionException.class)
                    .hasMessageContaining("Connection is not available, request timed out").hasNoCause();
            assertThat(timeout.getSQLState()).isNull();
            assertThat(classify(timeout)).isEqualTo(FailureKind.POOL_EXHAUSTED);
            assertThat(held.isValid(1)).isTrue();
        }
    }

    @Test
    void tomcatPoolExhaustedIsAnExhaustedPool() throws Exception {
        org.apache.tomcat.jdbc.pool.DataSource pool = new org.apache.tomcat.jdbc.pool.DataSource();
        pool.setUrl(Pools.memoryDatabase());
        pool.setDriverClassName("org.h2.Driver");
        pool.setMaxActive(1);
        pool.setInitialSize(0);
        pool.setMaxWait(200);
        try (Connection held = pool.getConnection()) {
            Throwable exhausted = catchThrowable(pool::getConnection);

            assertThat(exhausted.getClass().getName()).isEqualTo("org.apache.tomcat.jdbc.pool.PoolExhaustedException");
            assertThat(classify(exhausted)).isEqualTo(FailureKind.POOL_EXHAUSTED);
            assertThat(held.isValid(1)).isTrue();
        } finally {
            pool.close();
        }
    }

    @Test
    void commonsPoolTimeoutAsDbcpReportsItIsAnExhaustedPool() throws Exception {
        GenericObjectPoolConfig<Object> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(1);
        config.setMaxWait(Duration.ofMillis(200));
        try (GenericObjectPool<Object> pool = new GenericObjectPool<>(new BasePooledObjectFactory<>() {
            @Override
            public Object create() {
                return new Object();
            }

            @Override
            public PooledObject<Object> wrap(Object object) {
                return new DefaultPooledObject<>(object);
            }
        }, config)) {
            Object held = pool.borrowObject();

            // The pool underneath Commons DBCP, really exhausted
            NoSuchElementException timeout = catchThrowableOfType(pool::borrowObject, NoSuchElementException.class);
            assertThat(timeout).hasMessageStartingWith("Timeout waiting for idle object");
            // And what PoolingDataSource.getConnection of DBCP makes of it
            SQLException fromDbcp = new SQLException("Cannot get a connection, pool error " + timeout.getMessage(), timeout);

            assertThat(classify(fromDbcp)).isEqualTo(FailureKind.POOL_EXHAUSTED);
            assertThat(classify(timeout)).isEqualTo(FailureKind.POOL_EXHAUSTED);
            pool.returnObject(held);
        }
    }

    // ---------------------------------------------------------------- a node that is not there

    @Test
    void postgresDriverThatFindsNobodyAtTheAddressIsAFailedConnection() {
        SQLException refused = catchThrowableOfType(
                () -> DriverManager.getConnection(Pools.absentPostgres(), "app", "secret"), SQLException.class);

        assertThat((Throwable) refused).isInstanceOf(PSQLException.class).hasRootCauseInstanceOf(ConnectException.class);
        assertThat(refused.getSQLState()).isEqualTo("08001");
        assertThat(classify(refused)).isEqualTo(FailureKind.CONNECTION_FAILED);
    }

    @Test
    void postgresDriverThatCannotResolveTheHostIsAFailedConnection() {
        SQLException unknownHost = catchThrowableOfType(
                () -> DriverManager.getConnection("jdbc:postgresql://no-such-host.invalid:5432/app", "app", "secret"), SQLException.class);

        assertThat((Throwable) unknownHost).isInstanceOf(PSQLException.class).hasRootCauseInstanceOf(UnknownHostException.class);
        assertThat(classify(unknownHost)).isEqualTo(FailureKind.CONNECTION_FAILED);
    }

    @Test
    void hikariTimeoutCausedByFailedCreationIsAFailedConnectionNotAnExhaustedPool() {
        try (HikariDataSource pool = Pools.hikari(Pools.absentPostgres(), 1)) {
            SQLException timeout = catchThrowableOfType(pool::getConnection, SQLException.class);

            // The same exception class and message as for an exhausted pool. The difference is the cause, which
            // is the failure of the driver, and the state HikariCP copies from it.
            assertThat((Throwable) timeout).isInstanceOf(SQLTransientConnectionException.class)
                    .hasMessageContaining("Connection is not available, request timed out")
                    .hasCauseInstanceOf(PSQLException.class);
            assertThat(timeout.getSQLState()).isEqualTo("08001");
            assertThat(classify(timeout)).isEqualTo(FailureKind.CONNECTION_FAILED);
        }
    }

    @Test
    void tomcatPoolThatCannotCreateAConnectionIsAFailedConnection() {
        org.apache.tomcat.jdbc.pool.DataSource pool = new org.apache.tomcat.jdbc.pool.DataSource();
        pool.setUrl(Pools.absentPostgres());
        pool.setDriverClassName("org.postgresql.Driver");
        pool.setInitialSize(0);
        pool.setMaxWait(200);
        try {
            assertThat(classify(catchThrowable(pool::getConnection))).isEqualTo(FailureKind.CONNECTION_FAILED);
        } finally {
            pool.close();
        }
    }

    @Test
    void connectionBrokenInTheMiddleOfASessionIsAFailedConnection() {
        // What the PostgreSQL driver throws when the socket dies under a statement
        PSQLException broken = new PSQLException("An I/O error occurred while sending to the backend.",
                PSQLState.CONNECTION_FAILURE, new IOException("Connection reset by peer"));

        assertThat(broken.getSQLState()).isEqualTo("08006");
        assertThat(classify(broken)).isEqualTo(FailureKind.CONNECTION_FAILED);
    }

    @Test
    void networkFailuresWithoutAnSqlExceptionAreFailedConnections() {
        assertThat(classify(new ConnectException("Connection refused"))).isEqualTo(FailureKind.CONNECTION_FAILED);
        assertThat(classify(new UnknownHostException("db.internal"))).isEqualTo(FailureKind.CONNECTION_FAILED);
        assertThat(classify(new NoRouteToHostException("No route to host"))).isEqualTo(FailureKind.CONNECTION_FAILED);
        assertThat(classify(new SocketTimeoutException("connect timed out"))).isEqualTo(FailureKind.CONNECTION_FAILED);
        assertThat(classify(new SocketException("Connection reset"))).isEqualTo(FailureKind.CONNECTION_FAILED);
        // Wrapped by whatever sits between the driver and the caller
        assertThat(classify(new IllegalStateException("could not open", new ConnectException("Connection refused"))))
                .isEqualTo(FailureKind.CONNECTION_FAILED);
    }

    @Test
    void jdbcConnectionExceptionsWithoutAStateAreFailedConnections() {
        assertThat(classify(new SQLNonTransientConnectionException("No operations allowed after connection closed")))
                .isEqualTo(FailureKind.CONNECTION_FAILED);
        // A transient connection failure that names a state of class 08 is about the node, whoever throws it
        assertThat(classify(new SQLTransientConnectionException("Could not connect to address=(host=db)(port=3306)", "08000")))
                .isEqualTo(FailureKind.CONNECTION_FAILED);
    }

    @Test
    void stateChainedWithSetNextExceptionCounts() {
        SQLException outer = new SQLException("Could not get a connection");
        outer.setNextException(new SQLException("The connection attempt failed.", "08001"));

        assertThat(classify(outer)).isEqualTo(FailureKind.CONNECTION_FAILED);
    }

    // ---------------------------------------------------------------- a node that refuses

    private static PSQLException serverError(String state, String message) {
        // The wire form of an ErrorResponse: fields of one letter and a value, each ended by a zero byte
        return new PSQLException(new ServerErrorMessage("SFATAL\u0000VFATAL\u0000C" + state + "\u0000M" + message + "\u0000\u0000"));
    }

    @Test
    void rejectedCredentialsAreAnAuthenticationFailure() {
        PSQLException wrongPassword = serverError("28P01", "password authentication failed for user \"app\"");
        PSQLException noAccess = serverError("28000", "no pg_hba.conf entry for host \"10.0.0.7\", user \"app\", database \"app\"");

        assertThat(wrongPassword.getSQLState()).isEqualTo("28P01");
        assertThat(classify(wrongPassword)).isEqualTo(FailureKind.AUTHENTICATION_FAILED);
        assertThat(classify(noAccess)).isEqualTo(FailureKind.AUTHENTICATION_FAILED);
        assertThat(classify(new SQLInvalidAuthorizationSpecException("Access denied for user 'app'")))
                .isEqualTo(FailureKind.AUTHENTICATION_FAILED);
        // A pool that timed out because every creation was refused reports the refusal as its cause
        assertThat(classify(new SQLTransientConnectionException("pool - Connection is not available, request timed out after 250ms.",
                "28P01", wrongPassword))).isEqualTo(FailureKind.AUTHENTICATION_FAILED);
    }

    // ---------------------------------------------------------------- everything else counts against the node

    @Test
    void whatIsNotRecognisedIsUnknownAndCountsAgainstTheNode() {
        // The server is up and will not take another connection: not a busy pool, the node cannot be used
        assertThat(classify(serverError("53300", "sorry, too many clients already"))).isEqualTo(FailureKind.UNKNOWN);
        assertThat(classify(serverError("57P03", "the database system is starting up"))).isEqualTo(FailureKind.UNKNOWN);
        assertThat(classify(new SQLException("syntax error at or near \"selct\"", "42601"))).isEqualTo(FailureKind.UNKNOWN);
        assertThat(classify(new SQLTimeoutException("The node did not answer within 1000 ms"))).isEqualTo(FailureKind.UNKNOWN);
        assertThat(classify(new IllegalStateException("The data source is closed"))).isEqualTo(FailureKind.UNKNOWN);
        assertThat(classify(new NoSuchElementException("No value present"))).isEqualTo(FailureKind.UNKNOWN);
        assertThat(classify(null)).isEqualTo(FailureKind.UNKNOWN);
    }

    @Test
    void transientConnectionFailureWithACauseThatSaysNothingIsNotTakenForLoad() {
        // The shape of a pool timeout, but something went wrong underneath and it is not known what
        SQLException unclear = new SQLTransientConnectionException("pool - Connection is not available",
                new IllegalStateException("driver not initialised"));

        assertThat(classify(unclear)).isEqualTo(FailureKind.UNKNOWN);
    }

    @Test
    void causeChainThatLoopsEnds() {
        IllegalStateException first = new IllegalStateException("first");
        IllegalStateException second = new IllegalStateException("second", first);
        first.initCause(second);

        assertThat(classify(first)).isEqualTo(FailureKind.UNKNOWN);
    }

    @Test
    void onlyAnExhaustedPoolLeavesTheNodeAlone() {
        assertThat(FailureKind.POOL_EXHAUSTED.nodeUnavailable()).isFalse();
        assertThat(FailureKind.CONNECTION_FAILED.nodeUnavailable()).isTrue();
        assertThat(FailureKind.AUTHENTICATION_FAILED.nodeUnavailable()).isTrue();
        assertThat(FailureKind.UNKNOWN.nodeUnavailable()).isTrue();
    }
}
