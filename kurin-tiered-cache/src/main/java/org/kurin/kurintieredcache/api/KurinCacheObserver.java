package org.kurin.kurintieredcache.api;

import java.time.Duration;

/**
 * Receives what the cache does, for metrics, logging or tracing of the application. Callbacks run on the
 * thread that performs the cache operation and must be fast. An exception thrown by an observer is logged
 * and otherwise ignored.
 */
public interface KurinCacheObserver {

    default void onLookup(String cache, String key, CacheResult result) {
    }

    /**
     * @param success false if the loader threw
     */
    default void onLoad(String cache, String key, Duration duration, boolean success) {
    }

    /**
     * @param key    the evicted key, or null when the whole cache was cleared
     * @param reason who asked for it, for example {@code "local"}, {@code "replicated"} or {@code "expired"}
     */
    default void onEvict(String cache, String key, String reason) {
    }

    /**
     * A caller waited for another load of the same key instead of loading itself.
     */
    default void onStampedeWait(String cache, String key, StampedeMode mode, Duration waited) {
    }

    /**
     * @param state {@code CLOSED}, {@code OPEN} or {@code HALF_OPEN}
     */
    default void onExternalStoreCircuit(String cache, String state) {
    }
}
