package org.kurin.kurintieredcache.provider;

import org.kurin.kurintieredcache.api.CacheTier;
import org.kurin.kurintieredcache.core.CacheConfigurationContext;
import org.kurin.kurintieredcache.core.CacheProvider;
import org.kurin.kurintieredcache.core.KurinCache;

public class RocksDBCacheProvider implements CacheProvider {
    @Override
    public boolean supports(CacheTier tier) {
        return tier == CacheTier.L2_ROCKSDB;
    }

    @Override
    public KurinCache createCache(String name, CacheConfigurationContext context) {
        return new L2RocksDBCache(name, context.storageBaseDir(), context.serializer());
    }
}
