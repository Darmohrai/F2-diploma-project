package org.kurin.actuator.boot3;

import org.kurin.kurinspringbootstarter.observability.KurinEndpointContributor;
import org.kurin.kurinspringbootstarter.starter.KurinProperties;
import org.kurin.kurintieredcache.core.KurinCacheHealth;
import org.kurin.kurintieredcache.core.KurinCacheManager;
import org.kurin.raft.KurinNode;
import org.kurin.raft.KurinNodeHealth;
import org.kurin.routing.health.KurinRoutingHealth;
import org.kurin.routing.spring.KurinRoutingDiagnostics;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.actuate.jdbc.DataSourceHealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Health indicators of Kurin for the Actuator of Spring Boot 3. Each part appears when the Kurin module it
 * reports on is in use, and can be switched off with {@code management.health.<name>.enabled=false}:
 * {@code kurin}, {@code kurin-cache}, {@code kurin-routing}, {@code db}.
 * <p>
 * The verdicts themselves are made by classes that know nothing of Spring Boot ({@link KurinNodeHealth},
 * {@link KurinCacheHealth}, {@link KurinRoutingHealth}); this class only puts them into the health model of
 * Spring Boot 3. {@code kurin-actuator-boot4} does the same for Spring Boot 4.
 */
@AutoConfiguration(
        afterName = {
                "org.kurin.kurinspringbootstarter.starter.KurinAutoConfiguration",
                "org.kurin.kurincachespringbootstarter.KurinCacheAutoConfiguration",
                "org.kurin.routing.spring.KurinRoutingAutoConfiguration"},
        // Ahead of the data source health of Spring Boot, which yields to a bean named dbHealthContributor
        beforeName = "org.springframework.boot.actuate.autoconfigure.jdbc.DataSourceHealthContributorAutoConfiguration")
@ConditionalOnClass(HealthIndicator.class)
public class KurinActuatorAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.kurin.kurinspringbootstarter.starter.KurinAutoConfiguration")
    static class NodeHealthConfiguration {
        static Health toHealth(KurinNodeHealth health) {
            Status status = switch (health.status()) {
                case UP -> Status.UP;
                case UNKNOWN -> Status.UNKNOWN;
                case OUT_OF_SERVICE -> Status.OUT_OF_SERVICE;
                case DOWN -> Status.DOWN;
            };
            return Health.status(status).withDetails(health.details()).build();
        }

        @Bean
        @ConditionalOnBean(KurinNode.class)
        @ConditionalOnMissingBean(name = "kurinHealthIndicator")
        @ConditionalOnEnabledHealthIndicator("kurin")
        HealthIndicator kurinHealthIndicator(KurinNode node, KurinProperties props) {
            return () -> toHealth(KurinNodeHealth.of(node, props.getHealth().getNoLeaderThreshold()));
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.kurin.kurintieredcache.core.KurinCacheManager")
    static class CacheHealthConfiguration {
        static Health toHealth(KurinCacheHealth health) {
            return Health.up().withDetail("degraded", health.degraded()).withDetail("caches", health.caches()).build();
        }

        @Bean
        @ConditionalOnBean(KurinCacheManager.class)
        @ConditionalOnMissingBean(name = "kurinCacheHealthIndicator")
        @ConditionalOnEnabledHealthIndicator("kurin-cache")
        HealthIndicator kurinCacheHealthIndicator(KurinCacheManager manager) {
            return () -> toHealth(KurinCacheHealth.of(manager));
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.kurin.routing.spring.KurinRoutingDiagnostics")
    @ConditionalOnBean(KurinRoutingDiagnostics.class)
    static class RoutingHealthConfiguration {
        static Health toHealth(KurinRoutingHealth health) {
            Status status = health.status() == KurinRoutingHealth.Status.OUT_OF_SERVICE ? Status.OUT_OF_SERVICE : Status.UP;
            return Health.status(status).withDetail("degraded", health.degraded()).withDetail("nodes", health.nodeStates())
                    .withDetail("shardWeights", health.weights()).build();
        }

        // A node that is down but tolerated is reported UP with degraded = true: it must not take the instance out of service
        static Health toHealth(KurinRoutingHealth.NodeHealth node) {
            return (node.blocking() ? Health.outOfService() : Health.up()).withDetails(node.details()).build();
        }

        @Bean
        @ConditionalOnMissingBean(name = "kurinRoutingHealthIndicator")
        @ConditionalOnEnabledHealthIndicator("kurin-routing")
        HealthIndicator kurinRoutingHealthIndicator(KurinRoutingDiagnostics routing) {
            return () -> toHealth(routing.health());
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = {
            "org.kurin.routing.spring.KurinRoutingDiagnostics",
            "org.springframework.boot.actuate.jdbc.DataSourceHealthIndicator",
            "org.springframework.jdbc.core.JdbcTemplate"})
    @ConditionalOnBean(KurinRoutingDiagnostics.class)
    static class DatabaseHealthConfiguration {
        /**
         * Replaces the {@code db} health of Spring Boot, which would ask the routing data source for a connection.
         * Without a shard there is none to give, so with several shards that check is DOWN for ever and takes
         * every instance out of the load balancer. This one reports the physical pools instead:
         * {@code <shard>.primary} and {@code <shard>.replica}, from what the health monitor of the routing
         * already knows, plus every data source of the application that is not behind the routing.
         */
        @Bean
        @ConditionalOnMissingBean(name = {"dbHealthIndicator", "dbHealthContributor"})
        @ConditionalOnEnabledHealthIndicator("db")
        HealthContributor dbHealthContributor(KurinRoutingDiagnostics routing, Map<String, DataSource> dataSources) {
            Map<String, HealthContributor> pools = new LinkedHashMap<>();
            routing.health().nodes().forEach((name, node) -> {
                if (routing.monitoringEnabled()) {
                    HealthIndicator fromMonitor = () -> RoutingHealthConfiguration.toHealth(routing.health().nodes().get(name));
                    pools.put(name, fromMonitor);
                } else {
                    // Nobody watches the pools: ask the database itself, as Spring Boot would
                    pools.put(name, new DataSourceHealthIndicator(routing.pool(node.shard(), node.role())));
                }
            });
            routing.dataSourcesOutsideOfRouting(dataSources).forEach((name, dataSource) ->
                    pools.putIfAbsent(name, new DataSourceHealthIndicator(dataSource)));
            return CompositeHealthContributor.fromMap(pools);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = {
            "org.kurin.routing.spring.KurinRoutingDiagnostics",
            "org.kurin.kurinspringbootstarter.observability.KurinEndpointContributor"})
    @ConditionalOnBean(KurinRoutingDiagnostics.class)
    static class RoutingEndpointConfiguration {
        /**
         * The routing starter does not depend on the node starter, so it cannot contribute to
         * {@code /actuator/kurin} itself. This module sees both.
         */
        @Bean
        @ConditionalOnMissingBean(name = "kurinRoutingEndpointContributor")
        KurinEndpointContributor kurinRoutingEndpointContributor(KurinRoutingDiagnostics routing) {
            return new KurinEndpointContributor() {
                @Override
                public String name() {
                    return "routing";
                }

                @Override
                public Object describe() {
                    return routing.describe();
                }
            };
        }
    }
}
