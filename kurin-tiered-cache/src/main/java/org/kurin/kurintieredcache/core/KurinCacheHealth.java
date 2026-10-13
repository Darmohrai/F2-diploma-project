package org.kurin.kurintieredcache.core;

import org.kurin.kurintieredcache.tier.CircuitBreaker;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * State of the caches of this instance, in a form that depends on no monitoring library.
 * <p>
 * The cache is never "down": the external tier is optional by design, and with its circuit open the cache keeps
 * working from memory, disk and the source. An open circuit is reported as {@code degraded}.
 *
 * @param degraded true while the circuit of an external store is not closed
 * @param caches   per cache: tiers, time to live, entries in memory, state of the external store
 */
public record KurinCacheHealth(boolean degraded, Map<String, Object> caches) {

    public static KurinCacheHealth of(KurinCacheManager manager) {
        boolean degraded = manager.caches().stream()
                .anyMatch(cache -> cache.externalCircuitState() != null && cache.externalCircuitState() != CircuitBreaker.State.CLOSED);
        return new KurinCacheHealth(degraded, describe(manager));
    }

    /**
     * @return what is known about every cache created so far; a new, modifiable map
     */
    public static Map<String, Object> describe(KurinCacheManager manager) {
        Map<String, Object> caches = new LinkedHashMap<>();
        for (TieredCache cache : manager.caches()) {
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("tiers", cache.tiers().toString());
            details.put("ttl", cache.settings().ttl().toString());
            details.put("memoryEntries", cache.memoryEntries());
            if (cache.externalCircuitState() != null) {
                details.put("externalStoreCircuit", cache.externalCircuitState().name());
                details.put("externalStoreShared", cache.hasSharedTier());
            }
            caches.put(cache.name(), details);
        }
        return caches;
    }
}
