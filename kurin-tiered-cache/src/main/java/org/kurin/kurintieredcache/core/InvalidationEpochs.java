package org.kurin.kurintieredcache.core;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Detects that a key was invalidated while a value for it was being loaded.
 * <p>
 * A read-through captures a {@link Token} before it asks the source, and stores the result only if the token
 * is still current afterwards. Without this, an instance that started loading just before a write could put
 * the old value back into the cache right after the invalidation removed it.
 * <p>
 * Epochs of single keys live in a bounded map. When an entry is pushed out, its epoch becomes the floor that
 * every key without an entry reports, so forgetting a key can only make a concurrent load skip its write,
 * never let a stale one through.
 */
public final class InvalidationEpochs {

    public record Token(long keyEpoch, long cacheEpoch) {
    }

    private final AtomicLong sequence = new AtomicLong();
    private final AtomicLong floor = new AtomicLong();
    private final Cache<String, Long> keyEpochs;
    private final Map<String, AtomicLong> cacheEpochs = new ConcurrentHashMap<>();

    public InvalidationEpochs(long maxTrackedKeys) {
        this.keyEpochs = Caffeine.newBuilder()
                .maximumSize(maxTrackedKeys)
                // Synchronous with the eviction, unlike a removal listener
                .evictionListener((String key, Long epoch, com.github.benmanes.caffeine.cache.RemovalCause cause) -> {
                    if (epoch != null) {
                        floor.accumulateAndGet(epoch, Math::max);
                    }
                })
                .build();
    }

    private static String compose(String cacheName, String key) {
        return cacheName + '\u0000' + key;
    }

    public void keyInvalidated(String cacheName, String key) {
        keyEpochs.put(compose(cacheName, key), sequence.incrementAndGet());
    }

    public void cacheInvalidated(String cacheName) {
        cacheEpochs.computeIfAbsent(cacheName, name -> new AtomicLong()).set(sequence.incrementAndGet());
    }

    public Token capture(String cacheName, String key) {
        return new Token(keyEpoch(cacheName, key), cacheEpoch(cacheName));
    }

    /**
     * @return true if neither the key nor its whole cache was invalidated since the token was captured
     */
    public boolean isCurrent(String cacheName, String key, Token token) {
        return token.keyEpoch() == keyEpoch(cacheName, key) && token.cacheEpoch() == cacheEpoch(cacheName);
    }

    private long keyEpoch(String cacheName, String key) {
        Long epoch = keyEpochs.getIfPresent(compose(cacheName, key));
        return epoch != null ? epoch : floor.get();
    }

    private long cacheEpoch(String cacheName) {
        AtomicLong epoch = cacheEpochs.get(cacheName);
        return epoch != null ? epoch.get() : 0;
    }
}
