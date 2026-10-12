package org.kurin.kurintieredcache.core;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.kurin.kurintieredcache.api.CacheTier;
import org.kurin.kurintieredcache.tier.CaffeineTier;
import org.kurin.kurintieredcache.tier.CircuitBreaker;
import org.kurin.kurintieredcache.tier.ExternalTier;
import org.kurin.kurintieredcache.tier.RocksDbTier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A named cache made of up to three tiers. Reads go down the tiers until one answers and copy the entry,
 * unchanged, into the faster tiers above it. Writes go to the local tiers at once and to the external tier
 * through its ordered queue.
 * <p>
 * Invalidation distinguishes tiers private to this instance (memory, disk, a non-shared external store) from
 * a shared external store, which only one instance needs to clean.
 */
public class TieredCache implements KurinCache {
    private static final Logger log = LoggerFactory.getLogger(TieredCache.class);
    private static final Duration SHARED_WRITE_MEMORY = Duration.ofMinutes(1);

    private final String name;
    private final CacheSettings settings;
    private final CaffeineTier memory;
    private final RocksDbTier disk;
    private final ExternalTier external;
    private final InvalidationEpochs epochs;
    private final ObserverHub observers;
    // Keys this instance recently wrote to the shared tier: if they are invalidated, it cleans up after itself
    private final Cache<String, Boolean> recentSharedWrites = Caffeine.newBuilder()
            .expireAfterWrite(SHARED_WRITE_MEMORY)
            .maximumSize(50_000)
            .build();

    public TieredCache(String name, CacheSettings settings, CaffeineTier memory, RocksDbTier disk, ExternalTier external,
                       InvalidationEpochs epochs, ObserverHub observers) {
        if (memory == null && disk == null && external == null) {
            throw new IllegalArgumentException("Cache '" + name + "' has no tier");
        }
        this.name = name;
        this.settings = settings;
        this.memory = memory;
        this.disk = disk;
        this.external = external;
        this.epochs = epochs;
        this.observers = observers;
    }

    @Override
    public String name() {
        return name;
    }

    public CacheSettings settings() {
        return settings;
    }

    /**
     * @return the tiers this cache really has, fastest first
     */
    public List<CacheTier> tiers() {
        List<CacheTier> tiers = new ArrayList<>(3);
        if (memory != null) {
            tiers.add(CacheTier.L1);
        }
        if (disk != null) {
            tiers.add(CacheTier.L2);
        }
        if (external != null) {
            tiers.add(CacheTier.L3);
        }
        return tiers;
    }

    public boolean hasSharedTier() {
        return external != null && external.isShared();
    }

    /**
     * @return the state of the circuit breaker of the external tier, or null if the cache has none
     */
    public CircuitBreaker.State externalCircuitState() {
        return external != null ? external.circuitState() : null;
    }

    public long memoryEntries() {
        return memory != null ? memory.estimatedSize() : 0;
    }

    // ---------------------------------------------------------------- reading

    @Override
    public CacheLookup get(String key) {
        if (memory != null) {
            CacheLookup lookup = memory.get(key);
            if (lookup.isHit()) {
                return lookup;
            }
        }
        if (disk != null) {
            CacheLookup lookup = readDisk(key);
            if (lookup.isHit()) {
                // Same entry, same expiry: a hit below does not extend the lifetime above
                if (memory != null) {
                    memory.put(key, lookup.entry());
                }
                return lookup;
            }
        }
        if (external != null) {
            CacheLookup lookup = external.get(key);
            if (lookup.isHit()) {
                if (memory != null) {
                    memory.put(key, lookup.entry());
                }
                writeDisk(key, lookup.entry());
                return lookup;
            }
        }
        return CacheLookup.miss();
    }

    private CacheLookup readDisk(String key) {
        try {
            return disk.get(key);
        } catch (RuntimeException e) {
            log.warn("Disk tier of cache '{}' failed on read of '{}': {}", name, key, e.getMessage());
            return CacheLookup.miss();
        }
    }

    // ---------------------------------------------------------------- writing

    @Override
    public void put(String key, CacheEntry entry) {
        putAndFlush(key, entry);
    }

    /**
     * Stores the entry in the local tiers synchronously.
     *
     * @return a future completed once the external tier was written too (at once if there is none). It never
     * fails. Waiting for it guarantees that other instances can see the entry in a shared store.
     */
    public CompletableFuture<Void> putAndFlush(String key, CacheEntry entry) {
        if (memory != null) {
            memory.put(key, entry);
        }
        writeDisk(key, entry);
        if (external == null) {
            return CompletableFuture.completedFuture(null);
        }
        if (external.isShared()) {
            recentSharedWrites.put(key, Boolean.TRUE);
        }
        return external.putAsync(key, entry);
    }

    private void writeDisk(String key, CacheEntry entry) {
        if (disk == null) {
            return;
        }
        try {
            disk.put(key, entry);
        } catch (RuntimeException e) {
            // The value stays usable from the other tiers
            log.warn("Value of '{}' in cache '{}' was not written to the disk tier: {}", key, name, e.getMessage());
        }
    }

    // ---------------------------------------------------------------- invalidation

    @Override
    public void evict(String key) {
        if (hasSharedTier()) {
            // Forgotten first, so that the local eviction does not delete from the shared tier a second time
            recentSharedWrites.invalidate(key);
            evictLocal(key, "local");
            external.evictAsync(key);
        } else {
            evictLocal(key, "local");
        }
    }

    /**
     * Removes the key from the tiers that belong to this instance alone.
     *
     * @param reason reported to observers
     */
    public void evictLocal(String key, String reason) {
        epochs.keyInvalidated(name, key);
        if (memory != null) {
            memory.evict(key);
        }
        if (disk != null) {
            try {
                disk.evict(key);
            } catch (RuntimeException e) {
                log.error("Key '{}' could not be removed from the disk tier of cache '{}': {}", key, name, e.getMessage());
            }
        }
        if (external != null) {
            // A private store is as local as memory. In a shared one this instance removes only what it may have
            // put there itself while the invalidation was on its way.
            if (!external.isShared() || recentSharedWrites.asMap().remove(key) != null) {
                external.evictAsync(key);
            }
        }
        observers.onEvict(name, key, reason);
    }

    /**
     * Removes the key from the shared external tier, if the cache has one.
     *
     * @return a future completed when the removal was attempted; it never fails
     */
    public CompletableFuture<Void> evictShared(String key) {
        if (!hasSharedTier()) {
            return CompletableFuture.completedFuture(null);
        }
        recentSharedWrites.invalidate(key);
        return external.evictAsync(key);
    }

    @Override
    public void clear() {
        if (hasSharedTier()) {
            recentSharedWrites.invalidateAll();
            clearLocal("local");
            external.clearAsync();
        } else {
            clearLocal("local");
        }
    }

    public void clearLocal(String reason) {
        epochs.cacheInvalidated(name);
        if (memory != null) {
            memory.clear();
        }
        if (disk != null) {
            try {
                disk.clear();
            } catch (RuntimeException e) {
                log.error("Disk tier of cache '{}' could not be cleared: {}", name, e.getMessage());
            }
        }
        if (external != null && (!external.isShared() || recentSharedWrites.estimatedSize() > 0)) {
            recentSharedWrites.invalidateAll();
            external.clearAsync();
        }
        observers.onEvict(name, null, reason);
    }

    public CompletableFuture<Void> clearShared() {
        if (!hasSharedTier()) {
            return CompletableFuture.completedFuture(null);
        }
        recentSharedWrites.invalidateAll();
        return external.clearAsync();
    }

    /**
     * Removes expired entries from the disk tier, a bounded number per call.
     */
    public int sweepDisk(int maxEntries) {
        return disk != null ? disk.sweepExpired(maxEntries) : 0;
    }

    @Override
    public void close() {
        if (memory != null) {
            memory.close();
        }
        if (disk != null) {
            disk.close();
        }
    }
}
