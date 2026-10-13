package org.kurin.routing.health;

import com.zaxxer.hikari.HikariDataSource;
import org.h2.tools.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kurin.routing.DataSourceType;
import org.kurin.routing.health.ConnectionFailureClassifier.FailureKind;
import org.kurin.routing.health.DataSourceHealthMonitor.NodeId;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * The probes against a database that is reached over the network and can be taken away: an H2 server on a
 * local port.
 */
class NodeProbeTest {
    private static final NodeId NODE = new NodeId("A", DataSourceType.REPLICA);
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    /**
     * Delegates to a pool, tells whatever URL the test wants it to tell, and counts what is borrowed.
     */
    private static class TellingPool implements DataSource {
        private final DataSource pool;
        private final String url;
        final AtomicInteger borrowed = new AtomicInteger();

        TellingPool(DataSource pool, String url) {
            this.pool = pool;
            this.url = url;
        }

        public String getJdbcUrl() {
            return url;
        }

        public String getUsername() {
            return "app";
        }

        public String getPassword() {
            return "secret";
        }

        @Override
        public Connection getConnection() throws SQLException {
            borrowed.incrementAndGet();
            return pool.getConnection();
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            borrowed.incrementAndGet();
            return pool.getConnection(username, password);
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return pool.getLogWriter();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            pool.setLogWriter(out);
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            pool.setLoginTimeout(seconds);
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return pool.getLoginTimeout();
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return pool.getParentLogger();
        }

        @Override
        public <T> T unwrap(Class<T> type) throws SQLException {
            return pool.unwrap(type);
        }

        @Override
        public boolean isWrapperFor(Class<?> type) throws SQLException {
            return pool.isWrapperFor(type);
        }
    }

    private Server server;
    private int port;
    private String url;
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @BeforeEach
    void startServer() throws Exception {
        port = Pools.closedPort();
        url = "jdbc:h2:tcp://127.0.0.1:" + port + "/mem:probe";
        startServerOnItsPort();
    }

    private void startServerOnItsPort() throws SQLException {
        server = Server.createTcpServer("-tcpPort", String.valueOf(port), "-ifNotExists").start();
    }

    @AfterEach
    void stop() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
        server.stop();
    }

    private <T extends AutoCloseable> T closedAfterwards(T closeable) {
        closeables.add(closeable);
        return closeable;
    }

    private HikariDataSource pool(String jdbcUrl) {
        HikariDataSource pool = closedAfterwards(Pools.hikari(jdbcUrl, 1));
        if (jdbcUrl.equals(url)) {
            // The database exists: its first connection gets the time a cold JVM may need
            Pools.awaitFirstConnection(pool);
        }
        return pool;
    }

    private DriverNodeProbe driverProbe() {
        DriverNodeProbe probe = new DriverNodeProbe();
        closeables.add(probe::close);
        return probe;
    }

    // Sessions the server has, not counting the one that asks
    private int sessionsOnTheServer() throws SQLException {
        try (Connection connection = DriverManager.getConnection(url, "app", "secret");
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("select count(*) from information_schema.sessions")) {
            result.next();
            return result.getInt(1) - 1;
        }
    }

    // ---------------------------------------------------------------- the driver probe

    @Test
    void driverProbeKeepsOneConnectionOfItsOwnAndBorrowsNothing() throws Exception {
        TellingPool pool = new TellingPool(pool(url), url);
        DriverNodeProbe probe = driverProbe();
        probe.register(NODE, pool);
        // What is there before the first check: the connection the pool keeps ready
        int withoutTheProbe = sessionsOnTheServer();

        for (int check = 0; check < 5; check++) {
            probe.check(NODE, pool, TIMEOUT);
        }

        assertThat(pool.borrowed.get()).isZero();
        // Five checks, one connection
        assertThat(sessionsOnTheServer()).isEqualTo(withoutTheProbe + 1);

        probe.close();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(sessionsOnTheServer()).isEqualTo(withoutTheProbe));
    }

    @Test
    void checkPassesWhileTheApplicationHoldsEveryConnectionOfThePool() throws Exception {
        TellingPool pool = new TellingPool(pool(url), url);
        DriverNodeProbe probe = driverProbe();
        probe.register(NODE, pool);

        try (Connection held = pool.getConnection()) {
            assertThatCode(() -> probe.check(NODE, pool, TIMEOUT)).doesNotThrowAnyException();
            assertThat(held.isValid(1)).isTrue();
        }
        // Only the application asked the pool
        assertThat(pool.borrowed.get()).isEqualTo(1);
    }

    @Test
    void nodeThatWentAwayFailsTheCheckAndThePoolIsNotAskedForASecondOpinion() throws Exception {
        TellingPool pool = new TellingPool(pool(url), url);
        DriverNodeProbe probe = driverProbe();
        probe.register(NODE, pool);
        probe.check(NODE, pool, TIMEOUT);

        server.stop();
        Throwable failure = catchThrowable(() -> probe.check(NODE, pool, TIMEOUT));

        assertThat(failure).isInstanceOf(SQLException.class);
        assertThat(ConnectionFailureClassifier.classify(failure)).isEqualTo(FailureKind.CONNECTION_FAILED);
        // The connection of the check has worked before, so its word is taken
        assertThat(pool.borrowed.get()).isZero();

        // And when the node is back, the probe finds it again
        startServerOnItsPort();
        assertThatCode(() -> probe.check(NODE, pool, TIMEOUT)).doesNotThrowAnyException();
    }

    @Test
    void connectionThatWentStaleBetweenChecksIsReplacedWithoutAFailedCheck() throws Exception {
        TellingPool pool = new TellingPool(pool(url), url);
        DriverNodeProbe probe = driverProbe();
        probe.register(NODE, pool);
        probe.check(NODE, pool, TIMEOUT);

        // The database restarts between two checks: the kept connection is dead, the node is not
        server.stop();
        startServerOnItsPort();

        assertThatCode(() -> probe.check(NODE, pool, TIMEOUT)).doesNotThrowAnyException();
        assertThat(pool.borrowed.get()).isZero();
    }

    @Test
    void nodeThatAcceptsTheConnectionAndSaysNothingFailsTheCheckAtTheTimeout() throws Exception {
        try (ServerSocket silent = new ServerSocket(0)) {
            String silentUrl = "jdbc:postgresql://127.0.0.1:" + silent.getLocalPort() + "/app";
            // The pool of the application is no better off
            TellingPool pool = new TellingPool(pool(Pools.absentPostgres()), silentUrl);
            DriverNodeProbe probe = driverProbe();
            probe.register(NODE, pool);
            long startedNanos = System.nanoTime();

            Throwable failure = catchThrowable(() -> probe.check(NODE, pool, Duration.ofMillis(300)));

            assertThat(failure).isInstanceOf(SQLTimeoutException.class).hasMessageContaining("A/REPLICA").hasMessageContaining("300 ms");
            assertThat(ConnectionFailureClassifier.classify(failure).nodeUnavailable()).isTrue();
            // Given up at the timeout, give or take a pause of the JVM; the driver alone would wait much longer
            assertThat(Duration.ofNanos(System.nanoTime() - startedNanos)).isLessThan(Duration.ofSeconds(8));
        }
    }

    // ---------------------------------------------------------------- falling back to the pool

    @Test
    void dataSourceThatDoesNotTellItsUrlIsCheckedThroughThePool() throws Exception {
        HikariDataSource hikari = pool(url);
        AtomicInteger borrowed = new AtomicInteger();
        // Shows nothing but the methods of DataSource
        DataSource opaque = (DataSource) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getConnection")) {
                        borrowed.incrementAndGet();
                    }
                    try {
                        return method.invoke(hikari, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        DriverNodeProbe probe = driverProbe();
        probe.register(NODE, opaque);

        probe.check(NODE, opaque, TIMEOUT);
        probe.check(NODE, opaque, TIMEOUT);

        assertThat(borrowed.get()).isEqualTo(2);
        assertThat(sessionsOnTheServer()).isEqualTo(1);
    }

    @Test
    void urlWithoutARegisteredDriverIsCheckedThroughThePool() throws Exception {
        TellingPool pool = new TellingPool(pool(url), "jdbc:nosuchdatabase://127.0.0.1/app");
        DriverNodeProbe probe = driverProbe();
        probe.register(NODE, pool);

        probe.check(NODE, pool, TIMEOUT);

        assertThat(pool.borrowed.get()).isEqualTo(1);
    }

    @Test
    void connectionOfTheCheckThatNeverWorkedWhileThePoolDoesHandsTheNodeOverToThePool() throws Exception {
        // The pool reaches the database; the URL it tells leads nowhere, as when the pool adds what the URL lacks
        TellingPool pool = new TellingPool(pool(url), Pools.absentPostgres());
        DriverNodeProbe probe = driverProbe();
        probe.register(NODE, pool);

        // Not a failed check: the node answers, only not to this probe
        assertThatCode(() -> probe.check(NODE, pool, TIMEOUT)).doesNotThrowAnyException();
        assertThat(pool.borrowed.get()).isEqualTo(1);

        probe.check(NODE, pool, TIMEOUT);
        probe.check(NODE, pool, TIMEOUT);
        assertThat(pool.borrowed.get()).isEqualTo(3);
    }

    @Test
    void connectionOfTheCheckThatNeverWorkedAndABusyPoolSettleNothing() throws Exception {
        HikariDataSource hikari = pool(url);
        TellingPool pool = new TellingPool(hikari, Pools.absentPostgres());
        DriverNodeProbe probe = driverProbe();
        probe.register(NODE, pool);

        try (Connection held = Pools.take(hikari)) {
            Throwable failure = catchThrowable(() -> probe.check(NODE, pool, TIMEOUT));

            // The monitor is shown the busy pool, which it does not hold against the node
            assertThat(ConnectionFailureClassifier.classify(failure)).isEqualTo(FailureKind.POOL_EXHAUSTED);
            assertThat(held.isValid(1)).isTrue();
        }
        // Still undecided: with the pool free again the question is asked once more, and answered
        assertThatCode(() -> probe.check(NODE, pool, TIMEOUT)).doesNotThrowAnyException();
    }

    @Test
    void connectionOfTheCheckThatNeverWorkedAndAPoolThatFailsTooIsAFailedCheck() {
        String absent = Pools.absentPostgres();
        TellingPool pool = new TellingPool(pool(absent), absent);
        DriverNodeProbe probe = driverProbe();
        probe.register(NODE, pool);

        Throwable failure = catchThrowable(() -> probe.check(NODE, pool, TIMEOUT));

        assertThat(failure).isInstanceOf(org.postgresql.util.PSQLException.class);
        assertThat(ConnectionFailureClassifier.classify(failure)).isEqualTo(FailureKind.CONNECTION_FAILED);
        assertThat(pool.borrowed.get()).isEqualTo(1);
    }

    @Test
    void parametersCanBeSuppliedForDataSourcesThatDoNotTellThem() throws Exception {
        DataSource opaque = (DataSource) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
                    throw new AssertionError("The pool must not be used: " + method.getName());
                });
        DriverNodeProbe probe = new DriverNodeProbe((node, dataSource) ->
                node.equals(NODE) ? Optional.of(new ConnectionParameters(url, "app", "secret")) : Optional.empty());
        closeables.add(probe::close);
        probe.register(NODE, opaque);

        assertThatCode(() -> probe.check(NODE, opaque, TIMEOUT)).doesNotThrowAnyException();
        assertThat(sessionsOnTheServer()).isEqualTo(1);
    }

    // ---------------------------------------------------------------- the pool probe

    @Test
    void poolProbeBorrowsValidatesAndReturnsTheConnection() throws Exception {
        TellingPool pool = new TellingPool(pool(url), url);
        PoolNodeProbe probe = new PoolNodeProbe();

        // A pool of one: the second check gets a connection only if the first gave its own back
        probe.check(NODE, pool, TIMEOUT);
        probe.check(NODE, pool, TIMEOUT);
        assertThat(pool.borrowed.get()).isEqualTo(2);

        server.stop();
        assertThat(ConnectionFailureClassifier.classify(catchThrowable(() -> probe.check(NODE, pool, TIMEOUT))).nodeUnavailable()).isTrue();
    }

    @Test
    void poolProbeReportsAConnectionThatIsNotValid() throws Exception {
        Connection invalid = (Connection) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, args) -> method.getName().equals("isValid") ? false : null);
        DataSource handsOutInvalid = (DataSource) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, args) -> invalid);

        assertThatThrownBy(() -> new PoolNodeProbe().check(NODE, handsOutInvalid, TIMEOUT))
                .isInstanceOf(SQLNonTransientConnectionException.class)
                .hasMessageContaining("A/REPLICA");
    }

    @Test
    void validationTimeoutIsRoundedUpToWholeSeconds() {
        assertThat(PoolNodeProbe.seconds(Duration.ofMillis(1))).isEqualTo(1);
        assertThat(PoolNodeProbe.seconds(Duration.ofSeconds(1))).isEqualTo(1);
        assertThat(PoolNodeProbe.seconds(Duration.ofMillis(1001))).isEqualTo(2);
        // Zero would mean "no limit" to Connection.isValid
        assertThat(PoolNodeProbe.seconds(Duration.ZERO)).isEqualTo(1);
    }

    // ---------------------------------------------------------------- reading the parameters

    @Test
    void parametersAreReadFromTheCommonPools() throws Exception {
        HikariDataSource hikari = pool(url);
        assertThat(ConnectionParameters.of(hikari)).contains(new ConnectionParameters(url, "app", "secret"));

        org.apache.tomcat.jdbc.pool.DataSource tomcat = new org.apache.tomcat.jdbc.pool.DataSource();
        tomcat.setUrl(url);
        tomcat.setUsername("app");
        tomcat.setPassword("secret");
        // Tomcat JDBC answers getPassword() with a notice; the password is in its pool properties
        assertThat(tomcat.getPassword()).isNotEqualTo("secret");
        assertThat(ConnectionParameters.of(tomcat)).contains(new ConnectionParameters(url, "app", "secret"));

        assertThat(ConnectionParameters.of(new NamedLikeDbcp())).contains(new ConnectionParameters("jdbc:h2:mem:named", "app", "secret"));
    }

    /**
     * The getters of Commons DBCP, where the user is {@code getUserName}.
     */
    public static class NamedLikeDbcp extends TellingPool {
        NamedLikeDbcp() {
            super(null, null);
        }

        @Override
        public String getJdbcUrl() {
            return null;
        }

        public String getUrl() {
            return "jdbc:h2:mem:named";
        }

        @Override
        public String getUsername() {
            return null;
        }

        public String getUserName() {
            return "app";
        }
    }

    public static class WithoutCredentials extends TellingPool {
        WithoutCredentials(String url) {
            super(null, url);
        }

        @Override
        public String getUsername() {
            return null;
        }

        @Override
        public String getPassword() {
            throw new IllegalStateException("The password is not to be read");
        }
    }

    @Test
    void whatADataSourceDoesNotTellStaysUnknown() {
        assertThat(ConnectionParameters.of(new WithoutCredentials(url))).contains(new ConnectionParameters(url, null, null));
        assertThat(ConnectionParameters.of(new WithoutCredentials(null))).isEmpty();
        assertThat(ConnectionParameters.of(new WithoutCredentials(" "))).isEmpty();
        assertThatThrownBy(() -> new ConnectionParameters(" ", "app", "secret")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void passwordStaysOutOfTheTextForm() {
        assertThat(new ConnectionParameters(url, "app", "secret").toString()).contains(url, "app").doesNotContain("secret");
    }
}
