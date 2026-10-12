package org.kurin.kurintieredcache.api;

import java.util.ArrayList;
import java.util.List;

/**
 * Records the cache lookups made by the current thread between {@link #begin()} and {@link #end()}.
 * An application can use it to report, per request, which caches answered and from which tier.
 */
public final class KurinCacheTrace {

    public record Lookup(String cache, String key, CacheResult result) {
    }

    private static final ThreadLocal<List<Lookup>> CURRENT = new ThreadLocal<>();

    private KurinCacheTrace() {
    }

    /**
     * Starts recording on the current thread, discarding an unfinished recording if there is one.
     */
    public static void begin() {
        CURRENT.set(new ArrayList<>());
    }

    /**
     * @return the lookups recorded so far; empty if no recording is active
     */
    public static List<Lookup> lookups() {
        List<Lookup> lookups = CURRENT.get();
        return lookups != null ? List.copyOf(lookups) : List.of();
    }

    /**
     * Stops recording.
     *
     * @return everything that was recorded
     */
    public static List<Lookup> end() {
        List<Lookup> lookups = lookups();
        CURRENT.remove();
        return lookups;
    }

    /**
     * Called by the cache. Does nothing unless a recording is active on this thread.
     */
    public static void record(String cache, String key, CacheResult result) {
        List<Lookup> lookups = CURRENT.get();
        if (lookups != null) {
            lookups.add(new Lookup(cache, key, result));
        }
    }
}
