package org.kurin.routing.spring;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kurin.routing.DataSourceType;
import org.kurin.routing.KurinDataSourceTopology;
import org.kurin.routing.health.DataSourceHealthMonitor;
import org.kurin.routing.health.DataSourceHealthMonitor.NodeId;
import org.kurin.routing.health.KurinRoutingHealth;
import org.kurin.routing.health.NodeProbe;
import org.kurin.routing.spring.RoutingIntegrationTest.Items;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLNonTransientConnectionException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.kurin.routing.KurinDataSourceTopology.DEFAULT_SHARD;

/**
 * 1.1.2: the settings and meters of the node health checks.
 */
class NodeHealthSettingsTest {

    /**
     * A probe that fails for the nodes the test names and counts how often it was asked.
     */
    static final class SwitchableProbe implements NodeProbe {
        final Set<NodeId> away = ConcurrentHashMap.newKeySet();
        final AtomicInteger checks = new AtomicInteger();
        final List<Duration> timeouts = new ArrayList<>();

        @Override
        public void check(NodeId node, DataSource dataSource, Duration timeout) throws Exception {
            checks.incrementAndGet();
            timeouts.add(timeout);
            if (away.contains(node)) {
                throw new SQLNonTransientConnectionException("Connection to " + node + " refused", "08001", new ConnectException("refused"));
            }
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class App {
        @Bean
        Items items(JdbcTemplate jdbc, ApplicationContext context) {
            return new Items(jdbc, context);
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OwnProbe {
        @Bean
        SwitchableProbe switchableProbe() {
            return new SwitchableProbe();
        }
    }

    private final List<HikariDataSource> pools = new ArrayList<>();
    private final String database = UUID.randomUUID().toString();

    @AfterEach
    void closePools() {
        pools.forEach(HikariDataSource::close);
    }

    private HikariDataSource pool(String node) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:" + database + "-" + node + ";DB_CLOSE_DELAY=-1");
        config.setMaximumPoolSize(1);
        // The shortest wait for a connection that HikariCP accepts
        config.setConnectionTimeout(250);
        HikariDataSource pool = new HikariDataSource(config);
        pools.add(pool);
        // Opening the first connection takes longer than that on a cold JVM: wait for it here
        await().atMost(Duration.ofSeconds(60)).until(() -> pool.getHikariPoolMXBean().getIdleConnections() > 0);
        JdbcTemplate jdbc = new JdbcTemplate(pool);
        jdbc.execute("create table marker (node varchar(32))");
        jdbc.execute("create table items (name varchar(64))");
        jdbc.update("insert into marker values (?)", node);
        return pool;
    }

    private ApplicationContextRunner runner(HikariDataSource primary, HikariDataSource replica) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KurinRoutingAutoConfiguration.class,
                        KurinRoutingObservabilityAutoConfiguration.class, AopAutoConfiguration.class,
                        RoutingIntegrationTest.boot("jdbc.DataSourceAutoConfiguration", "jdbc.autoconfigure.DataSourceAutoConfiguration"),
                        RoutingIntegrationTest.boot("jdbc.DataSourceTransactionManagerAutoConfiguration",
                                "jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration"),
                        RoutingIntegrationTest.boot("jdbc.JdbcTemplateAutoConfiguration", "jdbc.autoconfigure.JdbcTemplateAutoConfiguration"),
                        RoutingIntegrationTest.boot("transaction.TransactionAutoConfiguration", "transaction.autoconfigure.TransactionAutoConfiguration")))
                .withBean(KurinDataSourceTopology.class, () -> KurinDataSourceTopology.of(primary, replica))
                .withUserConfiguration(App.class)
                .withPropertyValues("kurin.routing.unrouted.mode=fail",
                        // A write of an earlier test on this thread must not keep the reads of this one on the primary
                        "kurin.routing.sticky-window=1ms", "kurin.routing.health.interval=1h");
    }

    private static double count(ApplicationContext context, String meter, String... tags) {
        Counter counter = context.getBean(MeterRegistry.class).find(meter).tags(tags).counter();
        return counter != null ? counter.count() : 0;
    }

    @Test
    void exhaustedPoolIsCountedPerNodeAndIsNotATransition() {
        HikariDataSource primary = pool("primary");
        HikariDataSource replica = pool("replica");
        runner(primary, replica).run(context -> {
            Items items = context.getBean(Items.class);

            try (Connection held = replica.getConnection()) {
                assertThat(catchThrowable(items::read)).isNotNull();
                assertThat(catchThrowable(items::read)).isNotNull();
                assertThat(held.isValid(1)).isTrue();
            }

            assertThat(count(context, "kurin.routing.pool.exhausted", "shard", DEFAULT_SHARD, "role", "REPLICA")).isEqualTo(2);
            assertThat(count(context, "kurin.routing.pool.exhausted", "shard", DEFAULT_SHARD, "role", "PRIMARY")).isZero();
            assertThat(count(context, "kurin.routing.node.transitions")).isZero();
            assertThat(context.getBean(MeterRegistry.class).get("kurin.routing.node.up").tags("role", "REPLICA").gauge().value()).isEqualTo(1);
        });
    }

    @Test
    void driverProbeIsTheDefaultAndChecksDoNotTouchTheExhaustedPool() {
        HikariDataSource primary = pool("primary");
        HikariDataSource replica = pool("replica");
        runner(primary, replica).run(context -> {
            DataSourceHealthMonitor monitor = context.getBean(DataSourceHealthMonitor.class);
            assertThat(context.getBean(KurinRoutingProperties.class).getHealth().getProbe()).isEqualTo(KurinRoutingProperties.Health.Probe.DRIVER);
            assertThat(context.getBean(KurinRoutingProperties.class).getHealth().getFailureThreshold()).isEqualTo(2);

            try (Connection held = replica.getConnection()) {
                monitor.checkAll();
                monitor.checkAll();
                assertThat(held.isValid(1)).isTrue();
            }

            assertThat(monitor.isUp(DEFAULT_SHARD, DataSourceType.REPLICA)).isTrue();
            // Nobody waited for the pool in vain
            assertThat(count(context, "kurin.routing.pool.exhausted")).isZero();
        });
    }

    @Test
    void poolProbeCanBeChosenAndABusyPoolStillDoesNotTakeTheNodeDown() {
        HikariDataSource primary = pool("primary");
        HikariDataSource replica = pool("replica");
        runner(primary, replica).withPropertyValues("kurin.routing.health.probe=pool").run(context -> {
            DataSourceHealthMonitor monitor = context.getBean(DataSourceHealthMonitor.class);

            try (Connection held = replica.getConnection()) {
                // More checks than the failure threshold, each of which waits for the pool and gets nothing
                monitor.checkAll();
                monitor.checkAll();
                monitor.checkAll();
                assertThat(held.isValid(1)).isTrue();
            }

            assertThat(monitor.isUp(DEFAULT_SHARD, DataSourceType.REPLICA)).isTrue();
            assertThat(count(context, "kurin.routing.pool.exhausted", "shard", DEFAULT_SHARD, "role", "REPLICA")).isEqualTo(3);
            assertThat(count(context, "kurin.routing.node.transitions")).isZero();
        });
    }

    @Test
    void failureThresholdAndTimeoutAreTakenFromThePropertiesAndTransitionsAreCounted() {
        HikariDataSource primary = pool("primary");
        HikariDataSource replica = pool("replica");
        runner(primary, replica).withUserConfiguration(OwnProbe.class)
                .withPropertyValues("kurin.routing.health.failure-threshold=3", "kurin.routing.health.recovery-threshold=2",
                        "kurin.routing.health.timeout=700ms")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    DataSourceHealthMonitor monitor = context.getBean(DataSourceHealthMonitor.class);
                    // The probe of the application replaced the built-in one
                    SwitchableProbe probe = context.getBean(SwitchableProbe.class);
                    NodeId replicaNode = new NodeId(DEFAULT_SHARD, DataSourceType.REPLICA);

                    probe.away.add(replicaNode);
                    monitor.checkAll();
                    monitor.checkAll();
                    assertThat(monitor.isUp(DEFAULT_SHARD, DataSourceType.REPLICA)).isTrue();
                    monitor.checkAll();
                    assertThat(monitor.isUp(DEFAULT_SHARD, DataSourceType.REPLICA)).isFalse();
                    assertThat(monitor.isUp(DEFAULT_SHARD, DataSourceType.PRIMARY)).isTrue();

                    KurinRoutingHealth.NodeHealth down = context.getBean(KurinRoutingDiagnostics.class).health().nodes().get("default.replica");
                    assertThat(down.details()).containsEntry("state", "DOWN").containsEntry("failureKind", "CONNECTION_FAILED");
                    assertThat(String.valueOf(down.details().get("failure"))).contains("refused");

                    probe.away.clear();
                    monitor.checkAll();
                    assertThat(monitor.isUp(DEFAULT_SHARD, DataSourceType.REPLICA)).isFalse();
                    monitor.checkAll();
                    assertThat(monitor.isUp(DEFAULT_SHARD, DataSourceType.REPLICA)).isTrue();

                    assertThat(count(context, "kurin.routing.node.transitions", "shard", DEFAULT_SHARD, "role", "REPLICA", "to", "down")).isEqualTo(1);
                    assertThat(count(context, "kurin.routing.node.transitions", "shard", DEFAULT_SHARD, "role", "REPLICA", "to", "up")).isEqualTo(1);
                    assertThat(count(context, "kurin.routing.node.transitions", "role", "PRIMARY")).isZero();
                    // Two nodes, five rounds
                    assertThat(probe.checks.get()).isEqualTo(10);
                    assertThat(probe.timeouts).containsOnly(Duration.ofMillis(700));
                });
    }

    @Test
    void newSettingsAreDescribedForTheIde() throws Exception {
        String metadata = new String(new ClassPathResource("META-INF/spring-configuration-metadata.json")
                .getInputStream().readAllBytes(), StandardCharsets.UTF_8);

        assertThat(metadata).contains("\"kurin.routing.health.failure-threshold\"", "\"kurin.routing.health.probe\"");
    }
}
