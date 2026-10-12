package org.kurin.kurintieredcache.core;

import org.kurin.kurintieredcache.api.CacheTier;

/**
 * Result of reading a key: the entry and the tier that had it, or a miss.
 *
 * @param entry the entry with its metadata, null on a miss
 * @param tier  the tier that answered, null on a miss
 */
public record CacheLookup(CacheEntry entry, CacheTier tier) {
    private static final CacheLookup MISS = new CacheLookup(null, null);

    public static CacheLookup miss() {
        return MISS;
    }

    public static CacheLookup hit(CacheEntry entry, CacheTier tier) {
        return new CacheLookup(entry, tier);
    }

    public boolean isHit() {
        return entry != null;
    }
}
