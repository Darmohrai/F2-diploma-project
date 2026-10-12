package org.kurin.kurintieredcache.api;

/**
 * Outcome of one cache lookup.
 */
public enum CacheResult {
    HIT_L1,
    HIT_L2,
    HIT_L3,
    /**
     * An expired value was returned while it is being refreshed in the background.
     */
    STALE,
    MISS;

    public static CacheResult hit(CacheTier tier) {
        return switch (tier) {
            case L1 -> HIT_L1;
            case L2 -> HIT_L2;
            case L3 -> HIT_L3;
        };
    }
}
