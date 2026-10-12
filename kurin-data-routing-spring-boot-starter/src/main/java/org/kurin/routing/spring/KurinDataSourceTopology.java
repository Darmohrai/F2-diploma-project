package org.kurin.routing.spring;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Describes the physical databases behind {@link KurinRoutingDataSource}: one or more shards,
 * each with a Primary pool and an optional Replica pool. Declaring a bean of this type
 * enables Kurin database routing.
 */
public class KurinDataSourceTopology {
    public static final String DEFAULT_SHARD = "default";

    public record Shard(DataSource primary, DataSource replica) {
    }

    private final Map<String, Shard> shards;

    private KurinDataSourceTopology(Map<String, Shard> shards) {
        this.shards = Collections.unmodifiableMap(shards);
    }

    /**
     * Topology without sharding: a single Primary/Replica pair.
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

    public static class Builder {
        private final Map<String, Shard> shards = new LinkedHashMap<>();

        /**
         * @param replica may be null, then reads of this shard go to its primary
         */
        public Builder shard(String name, DataSource primary, DataSource replica) {
            if (primary == null) {
                throw new IllegalArgumentException("Primary DataSource is required for shard: " + name);
            }
            if (shards.putIfAbsent(name, new Shard(primary, replica)) != null) {
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
