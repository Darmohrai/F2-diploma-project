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
import org.kurin.routing.KurinPrimaryUnavailableException;
import org.kurin.routing.health.DataSourceHealthMonitor;
import org.kurin.routing.spring.RoutingIntegrationTest.Events;
import org.kurin.routing.spring.RoutingIntegrationTest.Items;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.time.Duration;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.kurin.routing.KurinDataSourceTopology.DEFAULT_SHARD;

/**
 * 1.1.2: under load a pool runs out of connections; the node behind it is in order. A request that got no
 * connection in time must fail as on any saturated pool, and nothing else may happen: no node marked down, no
 * read moved to the primary, no "replica down" in the metrics.
 * <p>
 * The pools are real HikariCP pools of one connection each, and the test itself is the long transaction that
 * holds it.
 */
class PoolExhaustionRoutingTest {

    @Configuration(proxyBeanMethods = false)
    static class App {
        @Bean
        Items items(JdbcTemplate jdbc, ApplicationContext context) {
            return new Items(jdbc, context);
        }

        @Bean
        Events events() {
            return new Events();
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    private final List<HikariDataSource> pools = new ArrayList<>();
    private final String database = UUID.randomUUID().toString();

    @AfterEach
    void closePools() {
        pools.forEach(HikariDataSource::close);
    }

    // A pool of one connection on a database that says which node it is
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
                // The test runs the health checks itself
                .withPropertyValues("kurin.routing.unrouted.mode=fail",
                        // A write of an earlier test on this thread must not keep the reads of this one on the primary
                        "kurin.routing.sticky-window=1ms", "kurin.routing.health.interval=1h");
    }

    private static double count(ApplicationContext context, String meter, String... tags) {
        Counter counter = context.getBean(MeterRegistry.class).find(meter).tags(tags).counter();
        return counter != null ? counter.count() : 0;
    }

    private static boolean isPoolTimeout(Throwable error) {
        for (Throwable link = error; link != null; link = link.getCause()) {
            if (link instanceof SQLTransientConnectionException && String.valueOf(link.getMessage()).contains("request timed out")) {
                return true;
            }
        }
        return false;
    }

    @Test
    void readThatFindsTheReplicaPoolExhaustedFailsAndTheReplicaStaysInUse() {
        HikariDataSource primary = pool("primary");
        HikariDataSource replica = pool("replica");
        runner(primary, replica).run(context -> {
            assertThat(context).hasNotFailed();
            Items items = context.getBean(Items.class);
            DataSourceHealthMonitor monitor = context.getBean(DataSourceHealthMonitor.class);
            assertThat(items.read()).isEqualTo("replica");

            try (Connection longTransaction = replica.getConnection()) {
                longTransaction.setAutoCommit(false);

                // A parallel read waits for the pool and gets its timeout, not an answer from somewhere else
                Throwable failure = catchThrowable(items::read);
                assertThat(failure).as("the read is not quietly moved to the primary").isNotNull();
                assertThat(isPoolTimeout(failure)).as("%s", failure).isTrue();

                // Checks made meanwhile do not need the pool and find the node in order
                monitor.checkAll();
                monitor.checkAll();
                monitor.checkAll();
                assertThat(monitor.isUp(DEFAULT_SHARD, DataSourceType.REPLICA)).isTrue();
            }

            assertThat(monitor.isUp(DEFAULT_SHARD, DataSourceType.REPLICA)).isTrue();
            assertThat(context.getBean(Events.class).seen).isEmpty();
            assertThat(count(context, "kurin.routing.fallbacks")).isZero();
            // The connection is back in the pool, and the next read is served by the replica as before
            assertThat(items.read()).isEqualTo("replica");
        });
    }

    @Test
    void writeThatFindsThePrimaryPoolExhaustedFailsWithThePoolTimeoutNotWithPrimaryUnavailable() {
        HikariDataSource primary = pool("primary");
        HikariDataSource replica = pool("replica");
        runner(primary, replica).run(context -> {
            Items items = context.getBean(Items.class);
            DataSourceHealthMonitor monitor = context.getBean(DataSourceHealthMonitor.class);

            try (Connection longTransaction = primary.getConnection()) {
                longTransaction.setAutoCommit(false);

                Throwable failure = catchThrowable(() -> items.write("order-1"));

                assertThat(isPoolTimeout(failure)).as("%s", failure).isTrue();
                for (Throwable link = failure; link != null; link = link.getCause()) {
                    assertThat(link).isNotInstanceOf(KurinPrimaryUnavailableException.class);
                }
            }

            assertThat(monitor.isUp(DEFAULT_SHARD, DataSourceType.PRIMARY)).isTrue();
            assertThat(context.getBean(Events.class).seen).isEmpty();
            // Not remembered as an outage: the next write goes through
            assertThat(items.write("order-2")).isEqualTo("primary");
        });
    }

    @Test
    void replicaThatReallyCannotBeConnectedToIsStillTakenOutAtOnce() {
        HikariDataSource primary = pool("primary");
        HikariDataSource replica = pool("replica");
        runner(primary, replica).run(context -> {
            Items items = context.getBean(Items.class);
            DataSourceHealthMonitor monitor = context.getBean(DataSourceHealthMonitor.class);

            // The pool of the replica is shut: asking it fails for a reason that has nothing to do with load
            replica.close();

            assertThat(items.read()).isEqualTo("primary");
            assertThat(monitor.isUp(DEFAULT_SHARD, DataSourceType.REPLICA)).isFalse();
            assertThat(context.getBean(Events.class).seen).containsExactly("KurinReplicaDownEvent:default");
            assertThat(count(context, "kurin.routing.fallbacks", "reason", "replica-error")).isEqualTo(1);
        });
    }
}
