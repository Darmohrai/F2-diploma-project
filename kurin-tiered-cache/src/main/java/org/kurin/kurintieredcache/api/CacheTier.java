package org.kurin.kurintieredcache.api;

/**
 * The levels a cache can be built from, fastest first.
 */
public enum CacheTier {
    /**
     * In-memory, per instance (Caffeine).
     */
    L1,
    /**
     * On local disk, per instance (RocksDB). Survives a restart of the instance.
     */
    L2,
    /**
     * An external store supplied by the application through {@code ExternalCacheStore}, typically shared by
     * all instances.
     */
    L3
}
