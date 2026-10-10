package org.kurin.kurintieredcache.provider;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.kurin.kurintieredcache.core.KurinCache;

import java.util.concurrent.TimeUnit;

public class L1CaffeineCache implements KurinCache {
    private final String name;
    private final Cache<String, Object> cache;

    public L1CaffeineCache(String name, long maxSize, long expireAfterAccessMinutes) {
        this.name = name;
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(expireAfterAccessMinutes, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object get(String key) {
        return cache.getIfPresent(key);
    }

    @Override
    public void put(String key, Object value) {
        cache.put(key, value);
    }

    @Override
    public void evict(String key) {
        cache.invalidate(key);
    }

    @Override
    public void clear() {
        cache.invalidateAll();
    }

    @Override
    public void close() {
        cache.invalidateAll();
    }
}
