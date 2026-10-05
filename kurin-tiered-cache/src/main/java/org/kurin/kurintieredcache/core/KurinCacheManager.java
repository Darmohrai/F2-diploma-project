package org.kurin.kurintieredcache.core;

import org.kurin.kurintieredcache.api.CacheTier;

public interface KurinCacheManager extends AutoCloseable {

    KurinCache getOrCreateCache(String cacheName, CacheTier tier);

    KurinCache getCache(String cacheName);

    @Override
    void close();
}
