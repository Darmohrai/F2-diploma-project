package org.kurin.actuator.boot4;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.kurin.api.KurinCommand;
import org.kurin.api.KurinService;
import org.kurin.kurincachespringbootstarter.KurinCacheAutoConfiguration;
import org.kurin.kurinspringbootstarter.observability.KurinEndpoint;
import org.kurin.kurinspringbootstarter.observability.KurinObservabilityAutoConfiguration;
import org.kurin.kurinspringbootstarter.starter.KurinAutoConfiguration;
import org.kurin.kurinspringbootstarter.starter.KurinNodeBuilderCustomizer;
import org.kurin.kurinspringbootstarter.template.KurinTemplate;
import org.kurin.kurintieredcache.core.KurinCacheManager;
import org.kurin.raft.KurinNode;
import org.kurin.routing.KurinDataSourceTopology;
import org.kurin.routing.health.DataSourceHealthMonitor;
import org.kurin.routing.spring.KurinRoutingAutoConfiguration;
import org.kurin.test.FaultInjectingStorage;
import org.kurin.test.TestPorts;
import org.springframework.boot.jdbc.autoconfigure.health.DataSourceHealthContributorAutoConfiguration;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.HealthContributors;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class KurinActuatorAdapterTest {

    public record Ping(String text) {
    }

    @KurinService(stateless = true)
    public static class PingService {
        @KurinCommand
        public String ping(Ping command) {
            return command.text();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class NodeApp {
        static final FaultInjectingStorage STORAGE = new FaultInjectingStorage();

        @Bean
        PingService pingService() {
            return new PingService();
        }

        @Bean
        KurinNodeBuilderCustomizer breakableStorage() {
            return builder -> builder.storageDecorator(STORAGE::wrap);
        }
    }

    private static ApplicationContextRunner nodeRunner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KurinAutoConfiguration.class, KurinObservabilityAutoConfiguration.class,
                        KurinActuatorAutoConfiguration.class))
                .withUserConfiguration(NodeApp.class)
                .withPropertyValues(
                        "kurin.node.port=" + TestPorts.freePort(),
                        "kurin.node.storage=memory",
                        "kurin.node.heartbeat-interval=30ms",
                        "kurin.node.election-timeout-min=300ms",
                        "kurin.node.election-timeout-max=600ms",
                        "kurin.node.storage-retry-min=20ms",
                        "kurin.node.storage-retry-max=100ms");
    }

    private static Health healthOf(HealthContributor contributor) {
        return ((HealthIndicator) contributor).health();
    }

    @Test
    void nodeHealthFollowsLeadershipAndStorage() {
        NodeApp.STORAGE.setBroken(false);
        nodeRunner().run(context -> {
            KurinNode node = context.getBean(KurinNode.class);
            HealthIndicator indicator = context.getBean("kurinHealthIndicator", HealthIndicator.class);
            await().atMost(Duration.ofSeconds(10)).until(() -> node.leadership().isPresent());

            Health up = indicator.health();
            assertThat(up.getStatus()).isEqualTo(Status.UP);
            assertThat(up.getDetails()).containsEntry("role", "LEADER").containsEntry("leaderId", node.nodeId());

            NodeApp.STORAGE.setBroken(true);
            assertThatThrownBy(() -> context.getBean(KurinTemplate.class).submitCommand(new Ping("x")).get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(org.kurin.api.exception.KurinStorageException.class);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
                assertThat(String.valueOf(indicator.health().getDetails().get("storageFailure"))).contains("Injected storage failure");
            });

            NodeApp.STORAGE.setBroken(false);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(indicator.health().getStatus()).isEqualTo(Status.UP));
        });
    }

    @Test
    void nodeWithoutALeaderGoesOutOfServiceAfterTheThreshold() {
        nodeRunner()
                .withPropertyValues(
                        "kurin.node.advertised-host=127.0.0.1",
                        // A peer that does not exist: no quorum, no leader, ever
                        "kurin.node.peers=127.0.0.1:" + TestPorts.freePort(),
                        "kurin.node.health.no-leader-threshold=500ms")
                .run(context -> {
                    HealthIndicator indicator = context.getBean("kurinHealthIndicator", HealthIndicator.class);
                    assertThat(indicator.health().getStatus()).isNotEqualTo(Status.UP);
                    await().atMost(Duration.ofSeconds(10)).until(() -> indicator.health().getStatus().equals(Status.OUT_OF_SERVICE));
                    assertThat(indicator.health().getDetails()).containsKey("leaderUnknownForMs");
                });
    }

    @Test
    void indicatorsObeyTheStandardSwitches() {
        nodeRunner().withPropertyValues("management.health.kurin.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean("kurinHealthIndicator"));
        nodeRunner().withPropertyValues("management.health.defaults.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean("kurinHealthIndicator"));
        nodeRunner().withPropertyValues("management.health.defaults.enabled=false", "management.health.kurin.enabled=true")
                .run(context -> assertThat(context).hasBean("kurinHealthIndicator"));
    }

    @Test
    void applicationCanReplaceAnIndicator() {
        nodeRunner().withBean("kurinHealthIndicator", HealthIndicator.class, () -> () -> Health.unknown().withDetail("own", true).build())
                .run(context -> {
                    assertThat(context).hasSingleBean(HealthIndicator.class);
                    assertThat(context.getBean("kurinHealthIndicator", HealthIndicator.class).health().getDetails()).containsEntry("own", true);
                });
    }

    @Test
    void cacheHealthIsUpAndListsTheCaches(@TempDir Path directory) {
        nodeRunner()
                .withConfiguration(AutoConfigurations.of(KurinCacheAutoConfiguration.class,
                        org.kurin.kurincachespringbootstarter.KurinCacheObservabilityAutoConfiguration.class))
                .withPropertyValues("kurin.cache.data-dir=" + directory.resolve("cache"))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    context.getBean(KurinCacheManager.class).getCache("accounts");
                    Health health = context.getBean("kurinCacheHealthIndicator", HealthIndicator.class).health();
                    assertThat(health.getStatus()).isEqualTo(Status.UP);
                    assertThat(health.getDetails()).containsEntry("degraded", false);
                    assertThat(String.valueOf(health.getDetails().get("caches"))).contains("accounts");

                    // The node endpoint lists the section the cache starter contributes
                    assertThat(context.getBean(KurinEndpoint.class).kurin()).containsKeys("node", "caches");
                });
        nodeRunner()
                .withConfiguration(AutoConfigurations.of(KurinCacheAutoConfiguration.class,
                        org.kurin.kurincachespringbootstarter.KurinCacheObservabilityAutoConfiguration.class))
                .withPropertyValues("kurin.cache.data-dir=" + directory.resolve("cache-off"), "management.health.kurin-cache.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean("kurinCacheHealthIndicator"));
    }

    // ---------------------------------------------------------------- routing

    /**
     * A database that can be switched off, like a server that stopped accepting connections.
     */
    static final class SwitchableDatabase extends AbstractDataSource {
        private final DriverManagerDataSource delegate;
        volatile boolean available = true;

        SwitchableDatabase(String name) {
            this.delegate = new DriverManagerDataSource("jdbc:h2:mem:" + name + "-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        }

        @Override
        public Connection getConnection() throws SQLException {
            if (!available) {
                throw new SQLException("Connection refused");
            }
            return delegate.getConnection();
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return getConnection();
        }
    }

    static final class Databases {
        final Map<String, SwitchableDatabase> nodes = new LinkedHashMap<>();

        SwitchableDatabase node(String name) {
            return nodes.computeIfAbsent(name, SwitchableDatabase::new);
        }

        KurinDataSourceTopology twoShards() {
            return KurinDataSourceTopology.builder()
                    .shard("eu", node("eu-primary"), node("eu-replica"))
                    .shard("us", node("us-primary"), node("us-replica"))
                    .build();
        }
    }

    private static ApplicationContextRunner routingRunner(Databases databases) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KurinRoutingAutoConfiguration.class, KurinActuatorAutoConfiguration.class,
                        DataSourceHealthContributorAutoConfiguration.class))
                .withBean(KurinDataSourceTopology.class, databases::twoShards)
                // The periodic check is replaced by explicit ones, so the test decides when the monitor looks
                .withPropertyValues("kurin.routing.health.interval=1h", "kurin.routing.health.recovery-threshold=1");
    }

    private static Map<String, Health> components(HealthContributor composite) {
        Map<String, Health> components = new LinkedHashMap<>();
        for (HealthContributors.Entry component : (CompositeHealthContributor) composite) {
            components.put(component.name(), healthOf(component.contributor()));
        }
        return components;
    }

    @Test
    void databaseHealthReportsThePhysicalPoolsInsteadOfTheRoutingDataSource() {
        Databases databases = new Databases();
        routingRunner(databases).run(context -> {
            assertThat(context).hasNotFailed();
            // The health of Spring Boot stood back: there is one db contributor, and it is not built on the routing data source
            assertThat(context.getBeanNamesForType(HealthContributor.class))
                    .containsExactlyInAnyOrder("dbHealthContributor", "kurinRoutingHealthIndicator");
            HealthContributor db = context.getBean("dbHealthContributor", HealthContributor.class);
            HealthIndicator routing = context.getBean("kurinRoutingHealthIndicator", HealthIndicator.class);
            DataSourceHealthMonitor monitor = context.getBean(DataSourceHealthMonitor.class);

            assertThat(components(db).keySet()).containsExactly("eu.primary", "eu.replica", "us.primary", "us.replica");
            assertThat(components(db).values()).allSatisfy(health -> assertThat(health.getStatus()).isEqualTo(Status.UP));
            assertThat(routing.health().getStatus()).isEqualTo(Status.UP);
            assertThat(routing.health().getDetails()).containsEntry("degraded", false);

            // A dead replica degrades the service and takes nothing out of it
            databases.node("eu-replica").available = false;
            monitor.checkAll();
            Health replica = components(db).get("eu.replica");
            assertThat(replica.getStatus()).isEqualTo(Status.UP);
            assertThat(replica.getDetails()).containsEntry("state", "DOWN").containsEntry("degraded", true);
            assertThat(String.valueOf(replica.getDetails().get("failure"))).contains("Connection refused");
            assertThat(components(db).get("eu.primary").getDetails()).containsEntry("state", "UP").doesNotContainKey("degraded");
            assertThat(routing.health().getStatus()).isEqualTo(Status.UP);
            assertThat(routing.health().getDetails()).containsEntry("degraded", true);

            // So does a dead primary, unless the application said it cannot do without
            databases.node("us-primary").available = false;
            monitor.checkAll();
            assertThat(components(db).get("us.primary").getStatus()).isEqualTo(Status.UP);
            assertThat(routing.health().getStatus()).isEqualTo(Status.UP);

            databases.node("eu-replica").available = true;
            databases.node("us-primary").available = true;
            monitor.checkAll();
            assertThat(components(db).values()).allSatisfy(health -> assertThat(health.getDetails()).containsEntry("state", "UP"));
            assertThat(routing.health().getDetails()).containsEntry("degraded", false);
        });
    }

    @Test
    void deadPrimaryTakesTheInstanceOutOfServiceWhenPrimariesAreRequired() {
        Databases databases = new Databases();
        routingRunner(databases).withPropertyValues("kurin.routing.health.primary-required=true").run(context -> {
            HealthContributor db = context.getBean("dbHealthContributor", HealthContributor.class);
            HealthIndicator routing = context.getBean("kurinRoutingHealthIndicator", HealthIndicator.class);

            databases.node("us-primary").available = false;
            context.getBean(DataSourceHealthMonitor.class).checkAll();

            assertThat(components(db).get("us.primary").getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
            assertThat(components(db).get("us.primary").getDetails()).containsEntry("degraded", false);
            assertThat(components(db).get("eu.primary").getStatus()).isEqualTo(Status.UP);
            assertThat(routing.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
        });
    }

    @Test
    void dataSourcesOutsideOfRoutingKeepTheirOrdinaryHealth() {
        Databases databases = new Databases();
        SwitchableDatabase audit = new SwitchableDatabase("audit");
        routingRunner(databases).withBean("auditDataSource", DataSource.class, () -> audit).run(context -> {
            HealthContributor db = context.getBean("dbHealthContributor", HealthContributor.class);
            assertThat(components(db).keySet()).containsExactly("eu.primary", "eu.replica", "us.primary", "us.replica", "auditDataSource");
            assertThat(components(db).get("auditDataSource").getStatus()).isEqualTo(Status.UP);

            audit.available = false;
            assertThat(components(db).get("auditDataSource").getStatus()).isEqualTo(Status.DOWN);
        });
    }

    @Test
    void withoutMonitoringThePoolsAreAskedDirectly() {
        Databases databases = new Databases();
        routingRunner(databases).withPropertyValues("kurin.routing.health.enabled=false").run(context -> {
            HealthContributor db = context.getBean("dbHealthContributor", HealthContributor.class);
            assertThat(components(db).get("eu.replica").getStatus()).isEqualTo(Status.UP);
            databases.node("eu-replica").available = false;
            assertThat(components(db).get("eu.replica").getStatus()).isEqualTo(Status.DOWN);
        });
    }

    @Test
    void databaseAndRoutingHealthObeyTheStandardSwitches() {
        Databases databases = new Databases();
        routingRunner(databases).withPropertyValues("management.health.db.enabled=false", "management.health.kurin-routing.enabled=false")
                .run(context -> assertThat(context.getBeanNamesForType(HealthContributor.class)).isEmpty());
    }

    @Test
    void routingContributesItsSectionToTheKurinEndpoint() {
        Databases databases = new Databases();
        nodeRunner()
                .withConfiguration(AutoConfigurations.of(KurinRoutingAutoConfiguration.class))
                .withBean(KurinDataSourceTopology.class, databases::twoShards)
                .withPropertyValues("kurin.routing.health.interval=1h")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    Map<String, Object> endpoint = context.getBean(KurinEndpoint.class).kurin();
                    assertThat(endpoint).containsKeys("node", "routing");
                    @SuppressWarnings("unchecked")
                    Map<String, Object> routing = (Map<String, Object>) endpoint.get("routing");
                    assertThat(routing).containsEntry("sharded", true).containsEntry("status", "UP").containsEntry("degraded", false)
                            .containsEntry("consistencyStrategy", "TIMESTAMP");
                    @SuppressWarnings("unchecked")
                    Map<String, Map<String, Object>> shards = (Map<String, Map<String, Object>>) routing.get("shards");
                    assertThat(shards.keySet()).containsExactly("eu", "us");
                    assertThat(shards.get("eu")).containsEntry("weight", 1).containsEntry("primary", Map.of("state", "UP"));
                    @SuppressWarnings("unchecked")
                    Map<String, Object> placement = (Map<String, Object>) routing.get("placement");
                    assertThat(placement).containsEntry("virtualNodes", 160).containsEntry("weights", Map.of("eu", 1, "us", 1));
                });
    }
}
