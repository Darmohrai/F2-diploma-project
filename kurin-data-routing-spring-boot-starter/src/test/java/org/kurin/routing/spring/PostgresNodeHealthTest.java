package org.kurin.routing.spring;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.kurin.routing.DataSourceType;
import org.kurin.routing.KurinDataSourceTopology;
import org.kurin.routing.health.ConnectionFailureClassifier;
import org.kurin.routing.health.ConnectionFailureClassifier.FailureKind;
import org.kurin.routing.health.DataSourceHealthMonitor;
import org.kurin.routing.health.KurinRoutingHealth;
import org.kurin.routing.spring.RoutingIntegrationTest.Items;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.kurin.routing.KurinDataSourceTopology.DEFAULT_SHARD;

/**
 * 1.1.2, against a real PostgreSQL in a container: load is not an outage, an outage is found within the promised
 * time, and the node is used again when it is back. Skipped where Docker is not available.
 * <p>
 * The container is bound to a fixed port of the host and is stopped and started as a container, so the database
 * goes away and comes back at the same address, as a restarted server does.
 */
class PostgresNodeHealthTest {
    private static final Duration INTERVAL = Duration.ofMillis(200);
    private static final Duration TIMEOUT = Duration.ofSeconds(1);
    private static final int FAILURE_THRESHOLD = 2;
    private static final int RECOVERY_THRESHOLD = 2;
    // What survives a pause of the JVM or of the Docker host between two looks at the state
    private static final Duration MARGIN = Duration.ofSeconds(3);

    private static GenericContainer<?> postgres;
    private static String url;

    private static TimeZone zoneOfTheJvm;

    @BeforeAll
    static void startPostgres() throws Exception {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available: this test needs a real PostgreSQL");
        // The driver hands the time zone of the JVM to the server, and the image does not know every name a
        // workstation may be set to
        zoneOfTheJvm = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        int port;
        try (ServerSocket free = new ServerSocket(0)) {
            port = free.getLocalPort();
        }
        postgres = new GenericContainer<>("postgres:16")
                .withEnv("POSTGRES_USER", "app")
                .withEnv("POSTGRES_PASSWORD", "secret")
                .withEnv("POSTGRES_DB", "app")
                .withExposedPorts(5432)
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withPortBindings(
                        new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(5432))))
                // The server starts twice: once to initialise the data directory, then for good
                .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 2));
        postgres.start();
        url = "jdbc:postgresql://127.0.0.1:" + port + "/app";
        try (Connection connection = DriverManager.getConnection(url, "app", "secret"); Statement statement = connection.createStatement()) {
            statement.execute("create table marker (node varchar(32))");
            statement.execute("create table items (name varchar(64))");
            statement.execute("insert into marker values ('postgres')");
        }
    }

    @AfterAll
    static void stopPostgres() {
        if (postgres != null) {
            postgres.stop();
        }
        if (zoneOfTheJvm != null) {
            TimeZone.setDefault(zoneOfTheJvm);
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

    private final List<HikariDataSource> pools = new ArrayList<>();

    @AfterEach
    void closePools() {
        pools.forEach(HikariDataSource::close);
    }

    // The primary and the "replica" are two pools of one connection each on the same server
    private HikariDataSource pool(String name) {
        HikariConfig config = new HikariConfig();
        config.setPoolName(name);
        config.setJdbcUrl(url);
        config.setUsername("app");
        config.setPassword("secret");
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(500);
        config.setInitializationFailTimeout(-1);
        HikariDataSource pool = new HikariDataSource(config);
        pools.add(pool);
        // A first connection can take longer than the pool lets a caller wait
        await().atMost(Duration.ofSeconds(60)).until(() -> pool.getHikariPoolMXBean().getIdleConnections() > 0);
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
                        "kurin.routing.sticky-window=1ms",
                        "kurin.routing.health.interval=" + INTERVAL.toMillis() + "ms",
                        "kurin.routing.health.timeout=" + TIMEOUT.toMillis() + "ms",
                        "kurin.routing.health.failure-threshold=" + FAILURE_THRESHOLD,
                        "kurin.routing.health.recovery-threshold=" + RECOVERY_THRESHOLD);
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
    void loadIsNotAnOutageAnOutageIsFoundInTimeAndTheNodeComesBack() {
        HikariDataSource primary = pool("primary");
        HikariDataSource replica = pool("replica");
        runner(primary, replica).run(context -> {
            assertThat(context).hasNotFailed();
            Items items = context.getBean(Items.class);
            DataSourceHealthMonitor monitor = context.getBean(DataSourceHealthMonitor.class);
            assertThat(items.read()).isEqualTo("postgres");

            // ---- A long transaction holds the only connection of the replica pool
            try (Connection longTransaction = replica.getConnection(); Statement statement = longTransaction.createStatement()) {
                longTransaction.setAutoCommit(false);
                statement.execute("select count(*) from items");

                Throwable failure = catchThrowable(items::read);
                assertThat(isPoolTimeout(failure)).as("%s", failure).isTrue();

                // Several rounds of checks pass meanwhile: the node stays in use
                await().during(INTERVAL.multipliedBy(6)).atMost(Duration.ofSeconds(20))
                        .until(() -> monitor.isUp(DEFAULT_SHARD, DataSourceType.REPLICA));
                longTransaction.rollback();
            }
            assertThat(count(context, "kurin.routing.pool.exhausted", "shard", DEFAULT_SHARD, "role", "REPLICA")).isEqualTo(1);
            assertThat(count(context, "kurin.routing.node.transitions")).isZero();
            assertThat(count(context, "kurin.routing.fallbacks")).isZero();
            assertThat(items.read()).isEqualTo("postgres");

            // ---- The server goes away
            postgres.getDockerClient().stopContainerCmd(postgres.getContainerId()).exec();
            Duration promised = INTERVAL.multipliedBy(FAILURE_THRESHOLD).plus(TIMEOUT);
            await("both nodes are found down").atMost(promised.plus(MARGIN)).pollInterval(Duration.ofMillis(20))
                    .until(() -> !monitor.isUp(DEFAULT_SHARD, DataSourceType.PRIMARY) && !monitor.isUp(DEFAULT_SHARD, DataSourceType.REPLICA));

            KurinRoutingHealth health = context.getBean(KurinRoutingDiagnostics.class).health();
            assertThat(health.nodes().get("default.replica").details())
                    .containsEntry("state", "DOWN").containsEntry("failureKind", "CONNECTION_FAILED");
            assertThat(health.nodes().get("default.primary").failureKind()).isEqualTo("CONNECTION_FAILED");

            // ---- And comes back at the same address
            postgres.getDockerClient().startContainerCmd(postgres.getContainerId()).exec();
            await("both nodes are used again").atMost(Duration.ofSeconds(60))
                    .until(() -> monitor.isUp(DEFAULT_SHARD, DataSourceType.PRIMARY) && monitor.isUp(DEFAULT_SHARD, DataSourceType.REPLICA));
            await("the pool has replaced its dead connection").atMost(Duration.ofSeconds(30)).ignoreExceptions()
                    .until(() -> "postgres".equals(items.read()));

            for (String role : List.of("PRIMARY", "REPLICA")) {
                assertThat(count(context, "kurin.routing.node.transitions", "shard", DEFAULT_SHARD, "role", role, "to", "down")).isEqualTo(1);
                assertThat(count(context, "kurin.routing.node.transitions", "shard", DEFAULT_SHARD, "role", role, "to", "up")).isEqualTo(1);
            }
        });
    }

    @Test
    void whatTheServerAnswersToAWrongPasswordIsAnAuthenticationFailure() {
        SQLException refused = catchThrowableOfType(() -> DriverManager.getConnection(url, "app", "not the password"), SQLException.class);

        assertThat(refused.getSQLState()).isEqualTo("28P01");
        assertThat(ConnectionFailureClassifier.classify(refused)).isEqualTo(FailureKind.AUTHENTICATION_FAILED);
    }
}
