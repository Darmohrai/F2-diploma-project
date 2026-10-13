package org.kurin.routing;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Describes the physical databases behind the routing data source: one or more shards, each with a primary
 * pool and an optional replica pool. Declaring a bean of this type enables Kurin data routing.
 */
public class KurinDataSourceTopology {
    public static final String DEFAULT_SHARD = "default";

    /**
     * @param replica may be null: reads of the shard then go to its primary
     * @param weight  relative share of the keys this shard should own; 1 for equal shards
     */
    public record Shard(DataSource primary, DataSource replica, int weight) {
    }

    private final Map<String, Shard> shards;

    private KurinDataSourceTopology(Map<String, Shard> shards) {
        this.shards = Collections.unmodifiableMap(shards);
    }

    /**
     * Topology without sharding: a single primary/replica pair.
     */
    public static KurinDataSourceTopology of(DataSource primary, DataSource replica) {
        return builder().shard(DEFAULT_SHARD, primary, replica).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, Shard> shards() {
        return shards;
    }

    public boolean isSharded() {
        return shards.size() > 1;
    }

    public static class Builder {
        private final Map<String, Shard> shards = new LinkedHashMap<>();

        public Builder shard(String name, DataSource primary, DataSource replica) {
            return shard(name, primary, replica, 1);
        }

        /**
         * @param weight relative share of the keys; a shard of weight 2 owns about twice as many as one of weight 1
         */
        public Builder shard(String name, DataSource primary, DataSource replica, int weight) {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("A shard needs a name");
            }
            if (primary == null) {
                throw new IllegalArgumentException("Primary DataSource is required for shard: " + name);
            }
            if (weight <= 0) {
                throw new IllegalArgumentException("Weight of shard " + name + " must be positive");
            }
            if (shards.putIfAbsent(name, new Shard(primary, replica, weight)) != null) {
                throw new IllegalArgumentException("Duplicate shard: " + name);
            }
            return this;
        }

        public KurinDataSourceTopology build() {
            if (shards.isEmpty()) {
                throw new IllegalArgumentException("At least one shard must be configured");
            }
            return new KurinDataSourceTopology(new LinkedHashMap<>(shards));
        }
    }
}
