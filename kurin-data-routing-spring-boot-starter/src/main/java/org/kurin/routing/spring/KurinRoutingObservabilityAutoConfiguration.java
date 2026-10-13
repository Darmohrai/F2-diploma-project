package org.kurin.routing.spring;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.kurin.routing.DataSourceType;
import org.kurin.routing.KurinDataSourceTopology;
import org.kurin.routing.health.DataSourceHealthMonitor;
import org.kurin.routing.shard.KurinShardOperations;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * Optional metrics of data routing. Its health indicators live in the Actuator adapter modules.
 * <p>
 * Meters: {@code kurin.routing.connections{shard,role}} (also the per-shard request count),
 * {@code kurin.routing.node.up{shard,role}}, {@code kurin.routing.fallbacks{shard,reason}},
 * {@code kurin.routing.fanout.duration}, {@code kurin.routing.fanout.failed.shards}.
 */
@AutoConfiguration(after = KurinRoutingAutoConfiguration.class, afterName = {
        // Spring Boot 3 names, then the names the same classes have in Spring Boot 4
        "org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.export.simple.SimpleMetricsExportAutoConfiguration",
        })
@ConditionalOnBean(KurinRoutingDataSource.class)
public class KurinRoutingObservabilityAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnBean(MeterRegistry.class)
    static class MetricsConfiguration {
        @Bean
        InitializingBean kurinRoutingMetrics(MeterRegistry registry, KurinRoutingDataSource routing, DataSourceHealthMonitor health,
                                             KurinDataSourceTopology topology, KurinShardOperations operations,
                                             org.springframework.beans.factory.ObjectProvider<org.kurin.routing.consistency.ReplicaPositionProbe> probes) {
            return () -> {
                // With a probe the lag is known exactly; it is read from the databases when the gauge is scraped
                probes.ifAvailable(probe -> topology.shards().forEach((name, shard) -> {
                    if (shard.replica() != null) {
                        Gauge.builder("kurin.routing.replica.lag", () -> {
                            try {
                                return Math.max(0, probe.currentPrimaryPosition(name, shard.primary())
                                        - probe.replicaReplayedPosition(name, shard.replica()));
                            } catch (java.sql.SQLException | RuntimeException e) {
                                return Double.NaN;
                            }
                        }).tag("shard", name).description("Replication positions the replica is behind its primary").register(registry);
                    }
                }));
                routing.addListener(new KurinRoutingDataSource.Listener() {
                    @Override
                    public void onConnection(String shard, DataSourceType role) {
                        registry.counter("kurin.routing.connections", "shard", shard, "role", role.name()).increment();
                    }

                    @Override
                    public void onFallback(String shard, String reason) {
                        registry.counter("kurin.routing.fallbacks", "shard", shard, "reason", reason).increment();
                    }
                });
                topology.shards().forEach((name, shard) -> {
                    for (DataSourceType role : DataSourceType.values()) {
                        if (role == DataSourceType.PRIMARY || shard.replica() != null) {
                            Gauge.builder("kurin.routing.node.up", health, monitor -> monitor.isUp(name, role) ? 1 : 0)
                                    .tag("shard", name).tag("role", role.name()).register(registry);
                        }
                    }
                });
                operations.addListener((shards, failed, duration) -> {
                    registry.timer("kurin.routing.fanout.duration").record(duration.toNanos(), TimeUnit.NANOSECONDS);
                    registry.counter("kurin.routing.fanout.failed.shards").increment(failed);
                });
            };
        }
    }

}
