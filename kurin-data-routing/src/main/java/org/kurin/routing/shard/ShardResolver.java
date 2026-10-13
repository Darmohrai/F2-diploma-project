package org.kurin.routing.shard;

import java.util.Set;

/**
 * Decides which shard owns a shard key. The built-in implementation uses weighted consistent hashing;
 * an application with its own placement rules (a directory table, ranges, tenants pinned to shards)
 * supplies its own.
 */
@FunctionalInterface
public interface ShardResolver {

    /**
     * @param shardKey the key, never null
     * @param shards   names of the configured shards
     * @return one of {@code shards}; the same one every time for the same key and set of shards
     */
    String resolve(Object shardKey, Set<String> shards);
}
