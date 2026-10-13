package org.kurin.routing.spring;

import org.kurin.routing.DataSourceType;
import org.kurin.routing.KurinDataSourceTopology;
import org.kurin.routing.consistency.ConsistencyTokenStrategy;
import org.kurin.routing.consistency.PostgresReplicaPositionProbe;
import org.kurin.routing.consistency.ReplicaPositionProbe;
import org.kurin.routing.consistency.ReplicaPositionTokenStrategy;
import org.kurin.routing.consistency.TimestampTokenStrategy;
import org.kurin.routing.health.DataSourceHealthMonitor;
import org.kurin.routing.shard.ConsistentHashShardResolver;
import org.kurin.routing.shard.KurinShardOperations;
import org.kurin.routing.shard.KurinShardRouter;
import org.kurin.routing.shard.ShardResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.util.ClassUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Activates when the application declares a {@link KurinDataSourceTopology} bean, and publishes the routing
 * {@code DataSource} as the primary one.
 */
// The data source autoconfiguration of Spring Boot 3, and the name the same class has in Spring Boot 4
@AutoConfiguration(beforeName = {
        "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
        "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration"})
@ConditionalOnClass(LazyConnectionDataSourceProxy.class)
@ConditionalOnBean(KurinDataSourceTopology.class)
@EnableAspectJAutoProxy
@EnableConfigurationProperties(KurinRoutingProperties.class)
public class KurinRoutingAutoConfiguration {
    private static final Logger log = LoggerFactory.getLogger(KurinRoutingAutoConfiguration.class);

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = {
            "org.springframework.web.context.request.ServletRequestAttributes",
            "jakarta.servlet.http.HttpServletRequest"
    })
    static class ServletClientIdConfiguration {
        @Bean
        @ConditionalOnMissingBean(KurinClientIdResolver.class)
        public KurinClientIdResolver servletKurinClientIdResolver() {
            return new ServletKurinClientIdResolver();
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public KurinClientIdResolver kurinClientIdResolver() {
        return () -> null;
    }

    /**
     * Weighted consistent hashing unless the application brings its own placement.
     */
    @Bean
    @ConditionalOnMissingBean
    public ShardResolver kurinShardResolver(KurinDataSourceTopology topology, KurinRoutingProperties props) {
        Map<String, Integer> weights = new LinkedHashMap<>();
        topology.shards().forEach((name, shard) -> weights.put(name, shard.weight()));
        props.getShards().forEach((name, shard) -> {
            if (!topology.shards().containsKey(name)) {
                throw new IllegalStateException("kurin.routing.shards." + name + " refers to a shard that is not in the topology "
                        + topology.shards().keySet());
            }
            if (shard.getWeight() != null) {
                weights.put(name, shard.getWeight());
            }
        });
        return new ConsistentHashShardResolver(weights, props.getVirtualNodes());
    }

    @Bean
    @ConditionalOnMissingBean
    public KurinShardRouter kurinShardRouter(KurinDataSourceTopology topology, ShardResolver resolver) {
        return new KurinShardRouter(topology.shards().keySet(), resolver);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public DataSourceHealthMonitor kurinDataSourceHealthMonitor(KurinDataSourceTopology topology, KurinRoutingProperties props,
                                                                ApplicationEventPublisher events) {
        KurinRoutingProperties.Health health = props.getHealth();
        DataSourceHealthMonitor monitor = new DataSourceHealthMonitor(health.getInterval(), health.getTimeout(), health.getRecoveryThreshold());
        topology.shards().forEach((name, shard) -> {
            monitor.register(name, DataSourceType.PRIMARY, shard.primary());
            if (shard.replica() != null) {
                monitor.register(name, DataSourceType.REPLICA, shard.replica());
            }
        });
        monitor.addListener((node, up, reason) -> {
            boolean primary = node.role() == DataSourceType.PRIMARY;
            events.publishEvent(primary
                    ? (up ? new KurinPrimaryUpEvent(monitor, node.shard(), reason) : new KurinPrimaryDownEvent(monitor, node.shard(), reason))
                    : (up ? new KurinReplicaUpEvent(monitor, node.shard(), reason) : new KurinReplicaDownEvent(monitor, node.shard(), reason)));
        });
        if (health.isEnabled()) {
            monitor.start();
        }
        return monitor;
    }

    @Bean
    @ConditionalOnMissingBean
    public ConsistencyTokenStrategy kurinConsistencyTokenStrategy(KurinRoutingProperties props, KurinClientIdResolver clientIdResolver,
                                                                  ObjectProvider<ReplicaPositionProbe> probes) {
        return switch (props.getConsistencyStrategy()) {
            case TIMESTAMP -> new TimestampTokenStrategy(props.getStickyWindow());
            case LOCAL_SESSION -> new LocalSessionStrategy(clientIdResolver, props.getStickyWindow());
            case REPLICA_POSITION -> {
                ReplicaPositionProbe probe = probes.getIfAvailable(() -> builtInProbe(props));
                yield new ReplicaPositionTokenStrategy(probe, props.getProbeCache());
            }
        };
    }

    private static ReplicaPositionProbe builtInProbe(KurinRoutingProperties props) {
        if (props.getPostgresProbe().isEnabled()) {
            if (!ClassUtils.isPresent("org.postgresql.Driver", KurinRoutingAutoConfiguration.class.getClassLoader())) {
                throw new IllegalStateException("kurin.routing.postgres-probe.enabled is set but the PostgreSQL driver is not on the classpath");
            }
            return new PostgresReplicaPositionProbe();
        }
        throw new IllegalStateException("kurin.routing.consistency-strategy=replica-position needs a ReplicaPositionProbe bean. "
                + "For PostgreSQL streaming replication set kurin.routing.postgres-probe.enabled=true.");
    }

    @Bean
    public KurinRoutingDataSource kurinRoutingDataSource(KurinDataSourceTopology topology, ConsistencyTokenStrategy strategy,
                                                         DataSourceHealthMonitor health, KurinRoutingProperties props) {
        return new KurinRoutingDataSource(topology, strategy, health, props.getReplicaFallback(), props.isReadonlyTransactionsToReplica());
    }

    /**
     * The data source the application sees. The physical connection is requested at the first statement, when
     * Spring has already published whether the surrounding transaction is read-only.
     */
    @Bean
    @Primary
    public DataSource kurinDataSource(KurinRoutingDataSource routing) {
        LazyConnectionDataSourceProxy lazy = new LazyConnectionDataSourceProxy();
        lazy.setTargetDataSource(routing);
        // Given explicitly, otherwise the proxy would open a connection at startup just to read them
        lazy.setDefaultAutoCommit(true);
        lazy.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        return lazy;
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public KurinShardOperations kurinShardOperations(KurinShardRouter router, KurinRoutingProperties props) {
        return new KurinShardOperations(router, props.getFanOut().getParallelism());
    }

    @Bean
    @ConditionalOnMissingBean
    public KurinRoutingDiagnostics kurinRoutingDiagnostics(KurinDataSourceTopology topology, DataSourceHealthMonitor monitor,
                                                           KurinRoutingProperties props, ShardResolver resolver,
                                                           ObjectProvider<ReplicaPositionProbe> probes) {
        return new KurinRoutingDiagnostics(topology, monitor, props, resolver, probes);
    }

    @Bean
    public KurinRoutingAspect kurinRoutingAspect(KurinShardRouter shardRouter) {
        return new KurinRoutingAspect(shardRouter);
    }

    /**
     * Carries the consistency token of web clients, for the strategies that use one.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    @ConditionalOnClass(name = {"org.springframework.web.filter.OncePerRequestFilter", "jakarta.servlet.Filter"})
    static class ServletTokenConfiguration {
        @Bean
        @ConditionalOnMissingBean
        ServletConsistencyTokenCarrier kurinConsistencyTokenCarrier(KurinRoutingProperties props) {
            return new ServletConsistencyTokenCarrier(props.getToken().getHeaderName(), props.getToken().getCookieName());
        }

        @Bean
        KurinConsistencyFilter kurinConsistencyFilter(ServletConsistencyTokenCarrier carrier, KurinRoutingProperties props) {
            if (props.getTokenSecret() == null || props.getTokenSecret().isEmpty()) {
                log.warn("kurin.routing.token-secret is not set: consistency tokens are not signed, so a client can alter "
                        + "its own token and with it whether its reads go to the primary or to a replica");
            }
            return new KurinConsistencyFilter(carrier, props.getTokenSecret());
        }
    }
}
