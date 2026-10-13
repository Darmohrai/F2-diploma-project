package org.kurin.routing;

import org.junit.jupiter.api.Test;
import org.kurin.routing.consistency.ConsistencyToken;
import org.kurin.routing.consistency.ReplicaPositionProbe;
import org.kurin.routing.consistency.ReplicaPositionTokenStrategy;
import org.kurin.routing.consistency.TimestampTokenStrategy;
import org.kurin.routing.health.DataSourceHealthMonitor;
import org.kurin.routing.health.PoolNodeProbe;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ConsistencyAndHealthTest {

    @Test
    void tokenRoundTripsAndRejectsTampering() {
        ConsistencyToken token = ConsistencyToken.empty().with("shard=A;x", "17").with("B", "9%");

        String signed = token.encode("secret");
        assertThat(ConsistencyToken.decode(signed, "secret")).contains(token);
        assertThat(ConsistencyToken.decode(signed, "other-secret")).isEmpty();
        assertThat(ConsistencyToken.decode(signed.substring(0, signed.length() - 2) + "00", "secret")).isEmpty();
        // The payload was replaced, the signature kept
        String forged = ConsistencyToken.empty().with("B", "999").encode(null) + signed.substring(signed.indexOf('.'));
        assertThat(ConsistencyToken.decode(forged, "secret")).isEmpty();
        // An unsigned token is not accepted where a signature is expected
        assertThat(ConsistencyToken.decode(token.encode(null), "secret")).isEmpty();

        assertThat(ConsistencyToken.decode(token.encode(null), null)).contains(token);
        assertThat(ConsistencyToken.decode("%%% not base64 %%%", null)).isEmpty();
        assertThat(ConsistencyToken.decode(null, "secret")).isEmpty();
        assertThat(ConsistencyToken.decode(ConsistencyToken.empty().encode("secret"), "secret")).contains(ConsistencyToken.empty());
    }

    @Test
    void timestampStrategyKeepsWritersOnThePrimaryForTheWindow() {
        TimestampTokenStrategy strategy = new TimestampTokenStrategy(Duration.ofMillis(300));

        assertThat(strategy.requiresPrimary(null, "A", null)).isFalse();
        String position = strategy.onWriteCommitted("A", null);
        assertThat(strategy.requiresPrimary(position, "A", null)).isTrue();
        await().atMost(Duration.ofSeconds(5)).until(() -> !strategy.requiresPrimary(position, "A", null));
        assertThat(strategy.requiresPrimary("garbage", "A", null)).isTrue();
    }

    @Test
    void positionStrategyReleasesTheWriterWhenTheReplicaCaughtUp() {
        AtomicLong primaryPosition = new AtomicLong(100);
        AtomicLong replicaPosition = new AtomicLong(40);
        AtomicInteger replicaProbes = new AtomicInteger();
        ReplicaPositionProbe probe = new ReplicaPositionProbe() {
            @Override
            public long currentPrimaryPosition(String shard, DataSource primary) {
                return primaryPosition.get();
            }

            @Override
            public long replicaReplayedPosition(String shard, DataSource replica) {
                replicaProbes.incrementAndGet();
                return replicaPosition.get();
            }
        };
        ReplicaPositionTokenStrategy strategy = new ReplicaPositionTokenStrategy(probe, Duration.ofMillis(100));

        String position = strategy.onWriteCommitted("A", null);
        assertThat(position).isEqualTo("100");
        assertThat(strategy.requiresPrimary(position, "A", null)).isTrue();
        // Within the cache period the replica is not asked again
        assertThat(strategy.requiresPrimary(position, "A", null)).isTrue();
        assertThat(replicaProbes.get()).isEqualTo(1);

        replicaPosition.set(100);
        await().atMost(Duration.ofSeconds(5)).until(() -> !strategy.requiresPrimary(position, "A", null));
        // Once reached, always reached: no further probes for this or an older position
        int probes = replicaProbes.get();
        assertThat(strategy.requiresPrimary(position, "A", null)).isFalse();
        assertThat(strategy.requiresPrimary("60", "A", null)).isFalse();
        assertThat(replicaProbes.get()).isEqualTo(probes);
        assertThat(strategy.requiresPrimary(null, "A", null)).isFalse();
    }

    /**
     * A DataSource whose connections are valid until it is switched off.
     */
    private static DataSource dataSource(AtomicBoolean available) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
            if (!method.getName().equals("getConnection")) {
                return null;
            }
            if (!available.get()) {
                throw new SQLException("connection refused");
            }
            return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                    (connection, connectionMethod, connectionArgs) -> connectionMethod.getName().equals("isValid") ? available.get() : null);
        });
    }

    @Test
    void monitorMarksNodesDownAtOnceAndUpOnlyAfterRepeatedSuccess() {
        AtomicBoolean primaryAvailable = new AtomicBoolean(true);
        AtomicBoolean replicaAvailable = new AtomicBoolean(true);
        List<String> changes = new ArrayList<>();
        // One failed check is enough here: the test is about recovery. The threshold has tests of its own.
        try (DataSourceHealthMonitor monitor = new DataSourceHealthMonitor(Duration.ofHours(1), Duration.ofSeconds(1), 1, 3,
                new PoolNodeProbe())) {
            monitor.register("A", DataSourceType.PRIMARY, dataSource(primaryAvailable));
            monitor.register("A", DataSourceType.REPLICA, dataSource(replicaAvailable));
            monitor.addListener((node, up, reason) -> changes.add(node + (up ? " up" : " down")));

            monitor.checkAll();
            assertThat(monitor.isUp("A", DataSourceType.PRIMARY)).isTrue();
            assertThat(changes).isEmpty();

            // A failure seen by the router takes effect immediately
            monitor.reportFailure("A", DataSourceType.REPLICA, new SQLException("timeout"));
            assertThat(monitor.isUp("A", DataSourceType.REPLICA)).isFalse();
            assertThat(monitor.isUp("A", DataSourceType.PRIMARY)).isTrue();

            // Two good checks are not enough with a threshold of three
            monitor.checkAll();
            monitor.checkAll();
            assertThat(monitor.isUp("A", DataSourceType.REPLICA)).isFalse();
            monitor.checkAll();
            assertThat(monitor.isUp("A", DataSourceType.REPLICA)).isTrue();

            // A failed check in between restarts the count
            primaryAvailable.set(false);
            monitor.checkAll();
            primaryAvailable.set(true);
            monitor.checkAll();
            monitor.checkAll();
            primaryAvailable.set(false);
            monitor.checkAll();
            primaryAvailable.set(true);
            monitor.checkAll();
            monitor.checkAll();
            assertThat(monitor.isUp("A", DataSourceType.PRIMARY)).isFalse();
            monitor.checkAll();
            assertThat(monitor.isUp("A", DataSourceType.PRIMARY)).isTrue();

            assertThat(changes).containsExactly("A/REPLICA down", "A/REPLICA up", "A/PRIMARY down", "A/PRIMARY up");
            assertThat(monitor.isUp("unknown", DataSourceType.PRIMARY)).isTrue();
        }
    }
}
