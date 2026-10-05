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
                Object value = currentTier.get(key);
                if (value != null) {
                    backfillTiers(key, value, i);
                    return value;
                }
            } catch (Exception e) {
                log.warn("[TieredCache] Fallback: Cache tier {} failed: {}. Falling back to the next tier.", currentTier.getName(), e.getMessage());
            }
        }
        return null;
    }

    private void backfillTiers(String key, Object value, int foundAtIndex) {
        for (int i = foundAtIndex - 1; i >= 0; i--) {
            try {
                tiers.get(i).put(key, value);
            } catch (Exception ignored) { }
        }
    }

    @Override
    public void put(String key, Object value) {
        CompletableFuture.runAsync(() -> {
            for (KurinCache tier : tiers) {
                try {
                    tier.put(key, value);
                } catch (Exception e) {
                    log.warn("[TieredCache] Error writing to tier {}: {}", tier.getName(), e.getMessage());
                }
            }
        });
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
            } catch (Exception ignored) { }
        }
    }
}
