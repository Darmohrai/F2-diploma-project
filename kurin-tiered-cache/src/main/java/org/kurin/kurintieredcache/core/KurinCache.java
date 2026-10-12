package org.kurin.kurintieredcache.core;

/**
 * A cache: one tier, or several tiers behind one facade.
 */
public interface KurinCache extends AutoCloseable {

    String name();

    /**
     * @return the entry and the tier it came from, or a miss. Entries past their time to live and stale window
     * are never returned.
     */
    CacheLookup get(String key);

    void put(String key, CacheEntry entry);

    void evict(String key);

    /**
     * Removes every entry of this cache.
     */
    void clear();

    @Override
    void close();
}
