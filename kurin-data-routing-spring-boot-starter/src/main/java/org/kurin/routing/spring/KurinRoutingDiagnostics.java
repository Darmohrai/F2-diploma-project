package org.kurin.routing.spring;

import org.kurin.routing.KurinDataSourceTopology;
import org.kurin.routing.consistency.ReplicaPositionProbe;
import org.kurin.routing.health.DataSourceHealthMonitor;
import org.kurin.routing.health.KurinRoutingHealth;
import org.kurin.routing.shard.ConsistentHashShardResolver;
import org.kurin.routing.shard.ShardResolver;
import org.springframework.beans.factory.ObjectProvider;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What the routing knows about its databases, for health indicators and management endpoints. It depends on no
 * monitoring library; the Actuator adapter modules translate it into the health and endpoint model of the
 * Spring Boot generation they are built for.
 */
public class KurinRoutingDiagnostics {
    private final KurinDataSourceTopology topology;
    private final DataSourceHealthMonitor monitor;
    private final KurinRoutingProperties props;
    private final ShardResolver resolver;
    private final ObjectProvider<ReplicaPositionProbe> probes;

    public KurinRoutingDiagnostics(KurinDataSourceTopology topology, DataSourceHealthMonitor monitor, KurinRoutingProperties props,
                                   ShardResolver resolver, ObjectProvider<ReplicaPositionProbe> probes) {
        this.topology = topology;
        this.monitor = monitor;
        this.props = props;
        this.resolver = resolver;
        this.probes = probes;
    }

    public KurinDataSourceTopology topology() {
        return topology;
    }

    /**
     * @return the physical pool of a database node
     * @throws IllegalArgumentException if the topology has no such node
     */
    public javax.sql.DataSource pool(String shard, org.kurin.routing.DataSourceType role) {
        KurinDataSourceTopology.Shard found = topology.shards().get(shard);
        javax.sql.DataSource pool = found == null ? null
                : role == org.kurin.routing.DataSourceType.PRIMARY ? found.primary() : found.replica();
        if (pool == null) {
            throw new IllegalArgumentException("The topology has no node " + shard + "/" + role);
        }
        return pool;
    }

    /**
     * @return false if the periodic checks of the database nodes are switched off; the monitor then knows only
     * about failures the router ran into
     */
    public boolean monitoringEnabled() {
        return props.getHealth().isEnabled();
    }

    /**
     * @return the state of every database node as the monitor last saw it; opens no connection
     */
    public KurinRoutingHealth health() {
        return KurinRoutingHealth.of(monitor, topology, props.getHealth().isPrimaryRequired());
    }

    /**
     * @return topology, node states, replica lag and placement settings, with values a JSON writer can handle.
     * The replica lag is read from the databases at the time of the call and only if a
     * {@link ReplicaPositionProbe} is available.
     */
    public Map<String, Object> describe() {
        KurinRoutingHealth health = health();
        ReplicaPositionProbe probe = probes.getIfAvailable();
        Map<String, Object> shards = new LinkedHashMap<>();
        topology.shards().forEach((name, shard) -> {
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("weight", shard.weight());
            details.put("primary", stateOf(health, name + ".primary"));
            if (shard.replica() != null) {
                details.put("replica", stateOf(health, name + ".replica"));
                if (probe != null) {
                    details.put("replicaLag", replicaLag(probe, name, shard));
                }
            }
            shards.put(name, details);
        });

        Map<String, Object> placement = new LinkedHashMap<>();
        placement.put("resolver", resolver.getClass().getName());
        if (resolver instanceof ConsistentHashShardResolver ring) {
            placement.put("virtualNodes", props.getVirtualNodes());
            placement.put("weights", ring.ringFor(topology.shards().keySet()).weights());
        }

        Map<String, Object> description = new LinkedHashMap<>();
        description.put("sharded", topology.isSharded());
        description.put("status", health.status().name());
        description.put("degraded", health.degraded());
        description.put("shards", shards);
        description.put("placement", placement);
        description.put("consistencyStrategy", props.getConsistencyStrategy().name());
        description.put("replicaFallback", props.getReplicaFallback().name());
        description.put("healthChecks", monitoringEnabled());
        return description;
    }

    /**
     * Picks, out of all {@code DataSource} beans of the application, those that are neither the routing data
     * source nor one of its pools. Their health has to be checked the ordinary way; the pools are covered by
     * {@link #health()}, and the routing data source itself cannot be checked without a shard.
     *
     * @param dataSources all {@code DataSource} beans by bean name
     */
    public Map<String, javax.sql.DataSource> dataSourcesOutsideOfRouting(Map<String, javax.sql.DataSource> dataSources) {
        java.util.Set<javax.sql.DataSource> pools = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        topology.shards().values().forEach(shard -> {
            pools.add(shard.primary());
            if (shard.replica() != null) {
                pools.add(shard.replica());
            }
        });
        Map<String, javax.sql.DataSource> others = new LinkedHashMap<>();
        dataSources.forEach((name, dataSource) -> {
            javax.sql.DataSource target = dataSource instanceof org.springframework.jdbc.datasource.DelegatingDataSource delegating
                    ? delegating.getTargetDataSource() : dataSource;
            if (!(target instanceof KurinRoutingDataSource) && !pools.contains(dataSource)) {
                others.put(name, dataSource);
            }
        });
        return others;
    }

    private static Map<String, Object> stateOf(KurinRoutingHealth health, String node) {
        KurinRoutingHealth.NodeHealth state = health.nodes().get(node);
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("state", state.up() ? "UP" : "DOWN");
        if (state.failure() != null) {
            details.put("failure", state.failure());
        }
        return details;
    }

    private static Object replicaLag(ReplicaPositionProbe probe, String name, KurinDataSourceTopology.Shard shard) {
        try {
            return Math.max(0, probe.currentPrimaryPosition(name, shard.primary()) - probe.replicaReplayedPosition(name, shard.replica()));
        } catch (java.sql.SQLException | RuntimeException e) {
            return "unknown: " + e.getMessage();
        }
    }
}
