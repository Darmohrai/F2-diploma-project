package org.kurin.kurintieredcache.core;

import org.kurin.kurintieredcache.api.CacheTier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Settings of one cache.
 *
 * @param ttl                  time to live of a value; zero means values never expire
 * @param nullTtl              time to live of a cached null; null means the same as {@code ttl}
 * @param jitterPercent        random spread of the time to live, 0 to 100
 * @param staleWhileRevalidate how long an expired value may still be served while it is refreshed; zero disables it
 * @param l1MaxSize            largest number of entries in the memory tier
 * @param tiers                the tiers of the cache, fastest first
 */
public record CacheSettings(Duration ttl, Duration nullTtl, int jitterPercent, Duration staleWhileRevalidate,
                            long l1MaxSize, List<CacheTier> tiers) {

    public CacheSettings {
        if (ttl == null || ttl.isNegative()) {
            throw new IllegalArgumentException("Cache ttl must be zero or positive");
        }
        if (jitterPercent < 0 || jitterPercent > 100) {
            throw new IllegalArgumentException("jitterPercent must be between 0 and 100, got " + jitterPercent);
        }
        if (staleWhileRevalidate == null || staleWhileRevalidate.isNegative()) {
            throw new IllegalArgumentException("staleWhileRevalidate must be zero or positive");
        }
        if (l1MaxSize <= 0) {
            throw new IllegalArgumentException("l1MaxSize must be positive");
        }
        if (tiers == null || tiers.isEmpty()) {
            throw new IllegalArgumentException("A cache needs at least one tier");
        }
        tiers = tiers.stream().distinct().sorted().toList();
    }

    public static CacheSettings defaults() {
        return new CacheSettings(Duration.ofMinutes(10), null, 0, Duration.ZERO, 10_000,
                List.of(CacheTier.L1, CacheTier.L2, CacheTier.L3));
    }

    public CacheSettings withTtl(Duration ttl) {
        return new CacheSettings(ttl, nullTtl, jitterPercent, staleWhileRevalidate, l1MaxSize, tiers);
    }

    public CacheSettings withNullTtl(Duration nullTtl) {
        return new CacheSettings(ttl, nullTtl, jitterPercent, staleWhileRevalidate, l1MaxSize, tiers);
    }

    public CacheSettings withJitterPercent(int jitterPercent) {
        return new CacheSettings(ttl, nullTtl, jitterPercent, staleWhileRevalidate, l1MaxSize, tiers);
    }

    public CacheSettings withStaleWhileRevalidate(Duration staleWhileRevalidate) {
        return new CacheSettings(ttl, nullTtl, jitterPercent, staleWhileRevalidate, l1MaxSize, tiers);
    }

    public CacheSettings withL1MaxSize(long l1MaxSize) {
        return new CacheSettings(ttl, nullTtl, jitterPercent, staleWhileRevalidate, l1MaxSize, tiers);
    }

    public CacheSettings withTiers(List<CacheTier> tiers) {
        return new CacheSettings(ttl, nullTtl, jitterPercent, staleWhileRevalidate, l1MaxSize, tiers);
    }

    /**
     * Builds the entry for a freshly loaded value: applies the time to live (the null one for a null value),
     * the jitter and the stale window.
     *
     * @param value the loaded value; null creates a null marker entry
     */
    public CacheEntry newEntry(Object value, long nowMs) {
        Duration base = value == null && nullTtl != null ? nullTtl : ttl;
        long ttlMs = base.toMillis();
        long expiresAtMs = 0;
        long staleUntilMs = 0;
        if (ttlMs > 0) {
            long spread = ttlMs * jitterPercent / 100;
            // Uniform over [ttl - spread, ttl + spread]
            long jittered = spread > 0 ? ttlMs - spread + ThreadLocalRandom.current().nextLong(2 * spread + 1) : ttlMs;
            expiresAtMs = nowMs + Math.max(1, jittered);
            if (!staleWhileRevalidate.isZero()) {
                staleUntilMs = expiresAtMs + staleWhileRevalidate.toMillis();
            }
        }
        return new CacheEntry(value != null ? value : KurinNullValue.INSTANCE, nowMs, expiresAtMs, staleUntilMs);
    }
}
