package org.kurin.routing.shard;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Places keys on shards by weighted consistent hashing (MD5 with virtual nodes). Adding or removing a shard
 * moves only the keys that go to or come from that shard.
 */
public class ConsistentHashShardResolver implements ShardResolver {
    private final Map<String, Integer> weights;
    private final int virtualNodes;
    private final Map<Set<String>, ConsistentHashRing> rings = new ConcurrentHashMap<>();

    /**
     * @param weights      weight per shard name; shards not listed weigh 1
     * @param virtualNodes points on the ring for a shard of weight 1
     */
    public ConsistentHashShardResolver(Map<String, Integer> weights, int virtualNodes) {
        this.weights = Map.copyOf(weights);
        this.virtualNodes = virtualNodes;
    }

    @Override
    public String resolve(Object shardKey, Set<String> shards) {
        return ringFor(shards).nodeFor(String.valueOf(shardKey));
    }

    /**
     * @return the ring used for the given set of shards, for inspection and for {@link ConsistentHashRing#diff}
     */
    public ConsistentHashRing ringFor(Set<String> shards) {
        return rings.computeIfAbsent(Set.copyOf(shards), names -> {
            Map<String, Integer> ringWeights = new LinkedHashMap<>();
            names.forEach(name -> ringWeights.put(name, weights.getOrDefault(name, 1)));
            return new ConsistentHashRing(ringWeights, virtualNodes);
        });
    }
}
