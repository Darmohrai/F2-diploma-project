package org.kurin.kurintieredcache.tier;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import org.kurin.kurintieredcache.api.CacheTier;
import org.kurin.kurintieredcache.core.CacheEntry;
import org.kurin.kurintieredcache.core.CacheLookup;
import org.kurin.kurintieredcache.core.KurinCache;

import java.util.concurrent.TimeUnit;

/**
 * The in-memory tier. Each entry lives exactly as long as its own metadata says.
 */
public class CaffeineTier implements KurinCache {
    private final String name;
    private final Cache<String, CacheEntry> cache;

    public CaffeineTier(String name, long maxSize) {
        this.name = name;
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfter(new Expiry<String, CacheEntry>() {
                    @Override
                    public long expireAfterCreate(String key, CacheEntry entry, long currentTime) {
                        return remainingNanos(entry);
                    }

                    @Override
                    public long expireAfterUpdate(String key, CacheEntry entry, long currentTime, long currentDuration) {
                        return remainingNanos(entry);
                    }

                    @Override
                    public long expireAfterRead(String key, CacheEntry entry, long currentTime, long currentDuration) {
                        return currentDuration;
                    }
                })
                .build();
    }

    private static long remainingNanos(CacheEntry entry) {
        long deadAtMs = entry.deadAtMs();
        if (deadAtMs == 0) {
            return Long.MAX_VALUE;
        }
        return TimeUnit.MILLISECONDS.toNanos(Math.max(0, deadAtMs - System.currentTimeMillis()));
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public CacheLookup get(String key) {
        CacheEntry entry = cache.getIfPresent(key);
        if (entry == null) {
            return CacheLookup.miss();
        }
        // Caffeine expires lazily; an entry that died a moment ago must not be handed out
        if (entry.isDead(System.currentTimeMillis())) {
            cache.asMap().remove(key, entry);
            return CacheLookup.miss();
        }
        return CacheLookup.hit(entry, CacheTier.L1);
    }

    @Override
    public void put(String key, CacheEntry entry) {
        cache.put(key, entry);
    }

    @Override
    public void evict(String key) {
        cache.invalidate(key);
    }

    @Override
    public void clear() {
        cache.invalidateAll();
    }

    public long estimatedSize() {
        return cache.estimatedSize();
    }

    @Override
    public void close() {
        cache.invalidateAll();
    }
}
