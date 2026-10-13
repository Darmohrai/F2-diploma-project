package org.kurin.routing.health;

import org.kurin.routing.DataSourceType;
import org.kurin.routing.KurinDataSourceTopology;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * State of the databases behind the router, in a form that depends on no monitoring library. It is read from the
 * {@link DataSourceHealthMonitor}, so asking for it opens no connection.
 *
 * @param status   overall verdict
 * @param degraded true while any node is down; reads of a shard with a dead replica are served by its primary
 * @param nodes    every physical pool, keyed {@code <shard>.primary} or {@code <shard>.replica}, in topology order
 * @param weights  weight of every shard
 */
public record KurinRoutingHealth(Status status, boolean degraded, Map<String, NodeHealth> nodes, Map<String, Integer> weights) {

    public enum Status {
        UP,
        /**
         * A primary is down and the application declared that it cannot work without its primaries.
         */
        OUT_OF_SERVICE
    }

    /**
     * @param failure  why the node is down, null while it is up
     * @param blocking true if the node is a primary that is down while the application cannot work without its
     *                 primaries; a node that is down and not blocking only degrades the service
     */
    public record NodeHealth(String shard, DataSourceType role, boolean up, String failure, boolean blocking) {
        /**
         * @return shard, role, state and, for a node that is down, the reason and whether the service is only degraded
         */
        public Map<String, Object> details() {
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("shard", shard);
            details.put("role", role.name());
            details.put("state", up ? "UP" : "DOWN");
            if (!up) {
                details.put("degraded", !blocking);
                details.put("failure", String.valueOf(failure));
            }
            return details;
        }
    }

    /**
     * @param primaryRequired report {@link Status#OUT_OF_SERVICE} when a primary is down. Without it the status
     *                        stays UP, because an application may prefer to keep serving reads from replicas.
     */
    public static KurinRoutingHealth of(DataSourceHealthMonitor monitor, KurinDataSourceTopology topology, boolean primaryRequired) {
        Map<String, NodeHealth> nodes = new LinkedHashMap<>();
        Map<String, Integer> weights = new LinkedHashMap<>();
        boolean anyDown = false;
        boolean primaryDown = false;
        for (Map.Entry<String, KurinDataSourceTopology.Shard> shard : topology.shards().entrySet()) {
            String name = shard.getKey();
            weights.put(name, shard.getValue().weight());
            for (DataSourceType role : DataSourceType.values()) {
                if (role == DataSourceType.REPLICA && shard.getValue().replica() == null) {
                    continue;
                }
                boolean up = monitor.isUp(name, role);
                nodes.put(name + "." + role.name().toLowerCase(java.util.Locale.ROOT),
                        new NodeHealth(name, role, up, monitor.failureOf(name, role),
                                !up && role == DataSourceType.PRIMARY && primaryRequired));
                anyDown |= !up;
                primaryDown |= !up && role == DataSourceType.PRIMARY;
            }
        }
        Status status = primaryDown && primaryRequired ? Status.OUT_OF_SERVICE : Status.UP;
        return new KurinRoutingHealth(status, anyDown, nodes, weights);
    }

    /**
     * @return the state of every node as {@code UP} or {@code DOWN}, keyed like {@link #nodes()}
     */
    public Map<String, Object> nodeStates() {
        Map<String, Object> states = new LinkedHashMap<>();
        nodes.forEach((name, node) -> states.put(name, node.up() ? "UP" : "DOWN"));
        return states;
    }
}
