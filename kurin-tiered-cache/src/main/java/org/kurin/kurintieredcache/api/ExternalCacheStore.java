package org.kurin.kurintieredcache.api;

import java.time.Duration;

/**
 * The external (L3) tier, implemented by the application on top of whatever store it runs: Redis, Memcached,
 * a database table. The framework encodes entries itself and hands the store opaque bytes, so an implementation
 * needs no serialization of its own and all values are stored in one format.
 * <p>
 * Keys have the form {@code kurin:<cacheName>:<key>}. Methods may block and may throw: every call is made
 * with a timeout and behind a circuit breaker, and a failure is treated as a miss.
 */
public interface ExternalCacheStore {

    /**
     * @return the stored bytes, or null if the key is absent
     */
    byte[] get(String key);

    /**
     * @param ttl time after which the store should drop the entry; null if the entry does not expire
     */
    void put(String key, byte[] value, Duration ttl);

    void delete(String key);

    /**
     * Removes every key that starts with the prefix. Used to clear a whole cache.
     */
    void deleteByPrefix(String prefix);

    /**
     * @return true if all instances of the application see the same data in this store. A shared store is
     * invalidated once, by the instance that changed the data; a private one is invalidated on every instance.
     */
    boolean isShared();
}
