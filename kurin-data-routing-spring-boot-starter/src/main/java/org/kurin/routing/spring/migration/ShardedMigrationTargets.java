package org.kurin.routing.spring.migration;

import org.kurin.routing.KurinDataSourceTopology;
import org.kurin.routing.spring.KurinRoutingDataSource;
import org.kurin.routing.spring.KurinRoutingProperties;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.jdbc.datasource.DelegatingDataSource;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Decides whether a migration tool that was pointed at the routing data source has to run on every shard
 * instead, and on which ones. Shared by the Liquibase and the Flyway support.
 */
final class ShardedMigrationTargets {
    private final BeanFactory beans;

    ShardedMigrationTargets(BeanFactory beans) {
        this.beans = beans;
    }

    /**
     * @param configured the data source the migration tool was given
     * @return the primaries to migrate, by shard name, in order; empty if the tool should be left alone: the
     * topology has one shard, the tool has a data source of its own, or the migrations per shard are switched off
     */
    Map<String, DataSource> primariesFor(DataSource configured) {
        if (!isRoutingDataSource(configured)) {
            return Map.of();
        }
        KurinDataSourceTopology topology = beans.getBean(KurinDataSourceTopology.class);
        KurinRoutingProperties props = beans.getBean(KurinRoutingProperties.class);
        if (!topology.isSharded() || !props.getMigrations().isEnabled()) {
            return Map.of();
        }
        List<String> chosen = props.getMigrations().getShards();
        Map<String, DataSource> primaries = new LinkedHashMap<>();
        if (chosen == null || chosen.isEmpty()) {
            topology.shards().forEach((name, shard) -> primaries.put(name, shard.primary()));
        } else {
            for (String name : chosen) {
                KurinDataSourceTopology.Shard shard = topology.shards().get(name);
                if (shard == null) {
                    throw new IllegalStateException("kurin.routing.migrations.shards names '" + name
                            + "', which is not a shard of the topology " + topology.shards().keySet());
                }
                primaries.put(name, shard.primary());
            }
        }
        return primaries;
    }

    private static boolean isRoutingDataSource(DataSource dataSource) {
        DataSource current = dataSource;
        // The application sees the routing data source behind a lazy proxy, possibly wrapped further
        for (int depth = 0; current instanceof DelegatingDataSource delegating && depth < 16; depth++) {
            current = delegating.getTargetDataSource();
        }
        return current instanceof KurinRoutingDataSource;
    }
}
