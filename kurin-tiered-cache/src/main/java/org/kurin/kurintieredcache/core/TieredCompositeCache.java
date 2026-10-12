package org.kurin.kurintieredcache.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class TieredCompositeCache implements KurinCache {
    private static final Logger log = LoggerFactory.getLogger(TieredCompositeCache.class);

    private final String name;
    private final List<KurinCache> tiers;

    public TieredCompositeCache(String name, List<KurinCache> tiers) {
        if (tiers == null || tiers.isEmpty()) {
            throw new IllegalArgumentException("Composite cache must have at least one tier");
        }
        this.name = name;
        this.tiers = tiers;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object get(String key) {
        for (int i = 0; i < tiers.size(); i++) {
            KurinCache currentTier = tiers.get(i);
            try {
                Object rawValue = currentTier.get(key);
                if (rawValue != null) {
                    if (rawValue instanceof CacheEntry entry) {
                        if (entry.isExpired()) {
                            log.debug("[TieredCache] Key '{}' expired in tier {}. Evicting.", key, currentTier.getName());
                            currentTier.evict(key);
                            continue;
                        }
                        backfillTiers(key, entry, i);
                        return entry.payload();
                    }
                    backfillTiers(key, rawValue, i);
                    return rawValue;
                }
            } catch (Exception e) {
                log.warn("[TieredCache] Fallback: Cache tier {} failed: {}. Falling back to the next tier.", currentTier.getName(), e.getMessage());
            }
        }
        return null;
    }

    @Override
    public void putWithJitter(String key, Object value, int jitterPercentage) {
        if (jitterPercentage <= 0) {
            put(key, value);
            return;
        }

        long baseTtlMs = 60 * 60 * 1000L;

        long jitterMs = (long) (baseTtlMs * (jitterPercentage / 100.0) * (Math.random() * 2 - 1));
        long expiresAt = System.currentTimeMillis() + baseTtlMs + jitterMs;

        CacheEntry entry = new CacheEntry(value, expiresAt);

        put(key, entry);
    }

    private void backfillTiers(String key, Object value, int foundAtIndex) {
        for (int i = foundAtIndex - 1; i >= 0; i--) {
            try {
                tiers.get(i).put(key, value);
            } catch (Exception ignored) {
            }
        }
    }

    @Override
    public void put(String key, Object value) {
        // The first tier is written synchronously so that a reader arriving right after the writer
        // (e.g. the next thread in the stampede lock queue) already sees the value.
        putToTier(tiers.get(0), key, value);

        if (tiers.size() > 1) {
            CompletableFuture.runAsync(() -> {
                for (KurinCache tier : tiers.subList(1, tiers.size())) {
                    putToTier(tier, key, value);
                }
            });
        }
    }

    private void putToTier(KurinCache tier, String key, Object value) {
        try {
            tier.put(key, value);
        } catch (Exception e) {
            log.warn("[TieredCache] Error writing to tier {}: {}", tier.getName(), e.getMessage());
        }
    }

    @Override
    public void evict(String key) {
        for (KurinCache tier : tiers) {
            try {
                tier.evict(key);
            } catch (Exception e) {
                log.warn("[TieredCache] Error evicting from tier {}: {}", tier.getName(), e.getMessage());
            }
        }
    }

    @Override
    public void clear() {
        for (KurinCache tier : tiers) {
            try {
                tier.clear();
            } catch (Exception e) {
                log.warn("[TieredCache] Error clearing tier {}: {}", tier.getName(), e.getMessage());
            }
        }
    }

    @Override
    public void close() {
        for (KurinCache tier : tiers) {
            try {
                tier.close();
            } catch (Exception ignored) {
            }
        }
    }
}
