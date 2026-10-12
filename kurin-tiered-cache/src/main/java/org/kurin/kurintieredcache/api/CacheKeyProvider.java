package org.kurin.kurintieredcache.api;

/**
 * Lets a command name the cache key it affects, for {@link KurinCachePut} and {@link KurinCacheEvict}
 * without a key expression.
 */
public interface CacheKeyProvider {
    String getCacheKey();
}
