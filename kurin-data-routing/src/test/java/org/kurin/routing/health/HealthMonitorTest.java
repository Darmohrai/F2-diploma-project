package org.kurin.routing.health;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kurin.routing.DataSourceType;
import org.kurin.routing.KurinDataSourceTopology;
import org.kurin.routing.health.ConnectionFailureClassifier.FailureKind;
import org.kurin.routing.health.DataSourceHealthMonitor.NodeId;

import javax.sql.DataSource;
import java.net.ConnectException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * 1.1.2: a pool without a free connection is not a node that is down, and one failed check is not an outage.
 */
class HealthMonitorTest {
    private static final Duration NEVER = Duration.ofHours(1);
    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    /**
     * A probe whose next answers the test dictates; no answer left means that the node is fine.
     */
    private static final class ScriptedProbe implements NodeProbe {
        final List<Exception> answers = new ArrayList<>();
        final List<String> calls = new ArrayList<>();
        int checks;

        // A null among the failures is a check that passes
        ScriptedProbe failing(Exception... failures) {
            answers.addAll(java.util.Arrays.asList(failures));
            return this;
        }

        @Override
        public void register(NodeId node, DataSource dataSource) {
            calls.add("register " + node);
        }

        @Override
        public void check(NodeId node, DataSource dataSource, Duration timeout) throws Exception {
            checks++;
            assertThat(timeout).isEqualTo(TIMEOUT);
            if (!answers.isEmpty()) {
                Exception next = answers.remove(0);
                if (next != null) {
                    throw next;
                }
            }
        }

        @Override
        public void close() {
            calls.add("close");
        }
    }

    private static final class Events implements DataSourceHealthMonitor.Listener {
        final List<String> seen = new ArrayList<>();

        @Override
        public void onStateChange(NodeId node, boolean up, String reason) {
            seen.add(node + (up ? " up" : " down"));
        }

        @Override
        public void onPoolExhausted(NodeId node, Throwable error) {
            seen.add(node + " pool exhausted");
        }
    }

    private static final SQLException REFUSED = new SQLNonTransientConnectionException("Connection refused", "08001",
            new ConnectException("Connection refused"));

    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void close() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private DataSourceHealthMonitor monitor(int failureThreshold, int recoveryThreshold, NodeProbe probe, Events events) {
        DataSourceHealthMonitor monitor = new DataSourceHealthMonitor(NEVER, TIMEOUT, failureThreshold, recoveryThreshold, probe);
        closeables.add(monitor);
        monitor.addListener(events);
        return monitor;
    }

    private HikariDataSource pool(String url, int size) {
        HikariDataSource pool = Pools.hikari(url, size);
        closeables.add(pool);
        return pool;
    }

    // What the pool of an application throws when all of its connections are in use
    private SQLException poolTimeout() throws SQLException {
        HikariDataSource pool = pool(Pools.memoryDatabase(), 1);
        try (Connection held = Pools.take(pool)) {
            return catchThrowableOfType(pool::getConnection, SQLException.class);
        }
    }

    // ---------------------------------------------------------------- periodic checks

    @Test
    void oneFailedCheckIsNotAnOutageAndTheThresholdCountsFailuresInARow() {
        ScriptedProbe probe = new ScriptedProbe();
        Events events = new Events();
        DataSourceHealthMonitor monitor = monitor(3, 2, probe, events);
        monitor.register("A", DataSourceType.REPLICA, null);

        // Two failures, a success, two failures: never three in a row
        probe.failing(REFUSED, REFUSED, null, REFUSED, REFUSED);
        for (int check = 0; check < 5; check++) {
            monitor.checkAll();
            assertThat(monitor.isUp("A", DataSourceType.REPLICA)).isTrue();
        }
        assertThat(monitor.failureOf("A", DataSourceType.REPLICA)).isNull();
        assertThat(monitor.failureKindOf("A", DataSourceType.REPLICA)).isNull();

        // The third in a row
        probe.failing(REFUSED);
        monitor.checkAll();
        assertThat(monitor.isUp("A", DataSourceType.REPLICA)).isFalse();
        assertThat(monitor.failureOf("A", DataSourceType.REPLICA)).contains("Connection refused");
        assertThat(monitor.failureKindOf("A", DataSourceType.REPLICA)).isEqualTo(FailureKind.CONNECTION_FAILED);
        assertThat(events.seen).containsExactly("A/REPLICA down");
    }

    @Test
    void defaultFailureThresholdIsTwo() {
        assertThat(DataSourceHealthMonitor.DEFAULT_FAILURE_THRESHOLD).isEqualTo(2);
    }

    @Test
    void nodeThatWasDownComesBackAfterRecoveryThresholdChecksAndThenNeedsTheFailureThresholdAgain() {
        ScriptedProbe probe = new ScriptedProbe();
        Events events = new Events();
        DataSourceHealthMonitor monitor = monitor(2, 2, probe, events);
        monitor.register("A", DataSourceType.PRIMARY, null);

        probe.failing(REFUSED, REFUSED);
        monitor.checkAll();
        monitor.checkAll();
        assertThat(monitor.isUp("A", DataSourceType.PRIMARY)).isFalse();

        // A failure between two successes restarts the count of successes
        probe.failing(null, REFUSED, null);
        monitor.checkAll();
        monitor.checkAll();
        monitor.checkAll();
        assertThat(monitor.isUp("A", DataSourceType.PRIMARY)).isFalse();
        monitor.checkAll();
        assertThat(monitor.isUp("A", DataSourceType.PRIMARY)).isTrue();
        assertThat(monitor.failureOf("A", DataSourceType.PRIMARY)).isNull();
        assertThat(monitor.failureKindOf("A", DataSourceType.PRIMARY)).isNull();

        // Back up, it is again not taken down by a single failed check
        probe.failing(REFUSED);
        monitor.checkAll();
        assertThat(monitor.isUp("A", DataSourceType.PRIMARY)).isTrue();
        assertThat(events.seen).containsExactly("A/PRIMARY down", "A/PRIMARY up");
    }

    @Test
    void checkThatOnlyFoundThePoolBusyChangesNothingInEitherDirection() throws Exception {
        SQLException busy = poolTimeout();
        ScriptedProbe probe = new ScriptedProbe();
        Events events = new Events();
        DataSourceHealthMonitor monitor = monitor(2, 2, probe, events);
        monitor.register("A", DataSourceType.REPLICA, null);

        // Up: a failed check, a busy pool, a failed check would be two failures in a row without the busy one
        // in between, and a busy pool is not a success either
        probe.failing(REFUSED, busy, busy, busy);
        for (int check = 0; check < 4; check++) {
            monitor.checkAll();
        }
        assertThat(monitor.isUp("A", DataSourceType.REPLICA)).isTrue();
        probe.failing(REFUSED);
        monitor.checkAll();
        assertThat(monitor.isUp("A", DataSourceType.REPLICA)).isFalse();

        // Down: busy pools do not count towards recovery
        probe.failing(null, busy, busy);
        for (int check = 0; check < 3; check++) {
            monitor.checkAll();
        }
        assertThat(monitor.isUp("A", DataSourceType.REPLICA)).isFalse();
        monitor.checkAll();
        assertThat(monitor.isUp("A", DataSourceType.REPLICA)).isTrue();

        assertThat(events.seen).containsExactly("A/REPLICA pool exhausted", "A/REPLICA pool exhausted", "A/REPLICA pool exhausted",
                "A/REPLICA down", "A/REPLICA pool exhausted", "A/REPLICA pool exhausted", "A/REPLICA up");
    }

    // ---------------------------------------------------------------- failures reported by the router

    @Test
    void requestThatCouldNotConnectTakesTheNodeDownAtOnce() {
        Events events = new Events();
        DataSourceHealthMonitor monitor = monitor(5, 2, new ScriptedProbe(), events);
        monitor.register("A", DataSourceType.REPLICA, null);
        monitor.register("A", DataSourceType.PRIMARY, null);

        FailureKind kind = monitor.reportConnectionFailure("A", DataSourceType.REPLICA, REFUSED);

        assertThat(kind).isEqualTo(FailureKind.CONNECTION_FAILED);
        assertThat(monitor.isUp("A", DataSourceType.REPLICA)).isFalse();
        assertThat(monitor.isUp("A", DataSourceType.PRIMARY)).isTrue();
        assertThat(monitor.failureKindOf("A", DataSourceType.REPLICA)).isEqualTo(FailureKind.CONNECTION_FAILED);
        // A failure of an unknown kind counts against the node as well; reportFailure is the same call without the verdict
        monitor.reportFailure("A", DataSourceType.PRIMARY, new IllegalStateException("the pool is closed"));
        assertThat(monitor.isUp("A", DataSourceType.PRIMARY)).isFalse();
        assertThat(monitor.failureKindOf("A", DataSourceType.PRIMARY)).isEqualTo(FailureKind.UNKNOWN);
        assertThat(events.seen).containsExactly("A/REPLICA down", "A/PRIMARY down");
    }

    @Test
    void requestThatFoundThePoolExhaustedLeavesTheNodeUpAndIsReported() throws Exception {
        SQLException busy = poolTimeout();
        Events events = new Events();
        DataSourceHealthMonitor monitor = monitor(2, 2, new ScriptedProbe(), events);
        monitor.register("A", DataSourceType.REPLICA, null);

        for (int request = 0; request < 50; request++) {
            assertThat(monitor.reportConnectionFailure("A", DataSourceType.REPLICA, busy)).isEqualTo(FailureKind.POOL_EXHAUSTED);
        }

        assertThat(monitor.isUp("A", DataSourceType.REPLICA)).isTrue();
        assertThat(monitor.failureOf("A", DataSourceType.REPLICA)).isNull();
        assertThat(events.seen).hasSize(50).containsOnly("A/REPLICA pool exhausted");
    }

    @Test
    void failureOfANodeThatIsNotRegisteredIsOnlyClassified() {
        Events events = new Events();
        DataSourceHealthMonitor monitor = monitor(2, 2, new ScriptedProbe(), events);

        assertThat(monitor.reportConnectionFailure("unknown", DataSourceType.PRIMARY, REFUSED)).isEqualTo(FailureKind.CONNECTION_FAILED);
        assertThat(monitor.isUp("unknown", DataSourceType.PRIMARY)).isTrue();
        assertThat(monitor.failureKindOf("unknown", DataSourceType.PRIMARY)).isNull();
        assertThat(events.seen).isEmpty();
    }

    @Test
    void listenerThatThrowsDoesNotKeepTheOthersFromHearing() throws Exception {
        Events events = new Events();
        DataSourceHealthMonitor monitor = new DataSourceHealthMonitor(NEVER, TIMEOUT, 1, 1, new ScriptedProbe());
        closeables.add(monitor);
        monitor.addListener(new DataSourceHealthMonitor.Listener() {
            @Override
            public void onStateChange(NodeId node, boolean up, String reason) {
                throw new IllegalStateException("broken listener");
            }

            @Override
            public void onPoolExhausted(NodeId node, Throwable error) {
                throw new IllegalStateException("broken listener");
            }
        });
        monitor.addListener(events);
        monitor.register("A", DataSourceType.REPLICA, null);

        monitor.reportFailure("A", DataSourceType.REPLICA, poolTimeout());
        monitor.reportFailure("A", DataSourceType.REPLICA, REFUSED);

        assertThat(events.seen).containsExactly("A/REPLICA pool exhausted", "A/REPLICA down");
    }

    // ---------------------------------------------------------------- the probe

    @Test
    void probeIsToldAboutEveryNodeAndClosedWithTheMonitor() {
        ScriptedProbe probe = new ScriptedProbe();
        DataSourceHealthMonitor monitor = new DataSourceHealthMonitor(NEVER, TIMEOUT, 2, 2, probe);
        monitor.register("A", DataSourceType.PRIMARY, null);
        monitor.register("A", DataSourceType.REPLICA, null);
        monitor.checkAll();

        monitor.close();

        assertThat(probe.calls).containsExactly("register A/PRIMARY", "register A/REPLICA", "close");
        assertThat(probe.checks).isEqualTo(2);
    }

    @Test
    void monitorWithoutAProbeOfItsOwnChecksPastAnExhaustedPool() throws Exception {
        // Counts how often the pool of the application is asked for a connection
        AtomicInteger borrowed = new AtomicInteger();
        HikariDataSource pool = new HikariDataSource(Pools.hikariConfig(Pools.memoryDatabase(), 1)) {
            @Override
            public Connection getConnection() throws SQLException {
                borrowed.incrementAndGet();
                return super.getConnection();
            }
        };
        closeables.add(pool);
        DataSourceHealthMonitor monitor = new DataSourceHealthMonitor(NEVER, TIMEOUT, 2);
        closeables.add(monitor);
        Events events = new Events();
        monitor.addListener(events);
        monitor.register("A", DataSourceType.PRIMARY, pool);

        // The application holds the only connection of the pool for as long as the checks run
        try (Connection held = Pools.take(pool)) {
            for (int check = 0; check < 5; check++) {
                monitor.checkAll();
            }
            assertThat(held.isValid(1)).isTrue();
        }

        assertThat(monitor.isUp("A", DataSourceType.PRIMARY)).isTrue();
        // One: the application itself. The checks went over a connection of their own.
        assertThat(borrowed.get()).isEqualTo(1);
        assertThat(events.seen).isEmpty();
    }

    @Test
    void healthVerdictCarriesTheKindOfTheFailure() {
        ScriptedProbe probe = new ScriptedProbe();
        DataSourceHealthMonitor monitor = monitor(1, 1, probe, new Events());
        DataSource primary = Pools.hikari(Pools.memoryDatabase(), 1);
        DataSource replica = Pools.hikari(Pools.memoryDatabase(), 1);
        closeables.add((AutoCloseable) primary);
        closeables.add((AutoCloseable) replica);
        KurinDataSourceTopology topology = KurinDataSourceTopology.of(primary, replica);
        monitor.register(KurinDataSourceTopology.DEFAULT_SHARD, DataSourceType.PRIMARY, primary);
        monitor.register(KurinDataSourceTopology.DEFAULT_SHARD, DataSourceType.REPLICA, replica);

        monitor.reportFailure(KurinDataSourceTopology.DEFAULT_SHARD, DataSourceType.REPLICA, REFUSED);
        KurinRoutingHealth health = KurinRoutingHealth.of(monitor, topology, false);

        KurinRoutingHealth.NodeHealth down = health.nodes().get("default.replica");
        assertThat(down.failureKind()).isEqualTo("CONNECTION_FAILED");
        assertThat(down.details()).containsEntry("state", "DOWN").containsEntry("failureKind", "CONNECTION_FAILED")
                .containsEntry("degraded", true).containsKey("failure");
        KurinRoutingHealth.NodeHealth up = health.nodes().get("default.primary");
        assertThat(up.failureKind()).isNull();
        assertThat(up.details()).containsOnlyKeys("shard", "role", "state");
        // The constructor of 1.1.1 still exists and leaves the kind out
        KurinRoutingHealth.NodeHealth legacy = new KurinRoutingHealth.NodeHealth("default", DataSourceType.REPLICA, false, "gone", false);
        assertThat(legacy.failureKind()).isNull();
        assertThat(legacy.details()).doesNotContainKey("failureKind").containsEntry("failure", "gone");
    }
}
