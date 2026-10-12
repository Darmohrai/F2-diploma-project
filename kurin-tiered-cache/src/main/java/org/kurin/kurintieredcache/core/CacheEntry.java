package org.kurin.kurintieredcache.core;

/**
 * A cached value with its lifetime. The same entry object travels through all tiers, so every tier expires it
 * at the same moment.
 *
 * @param payload      the value, or {@link KurinNullValue#INSTANCE} for a cached null
 * @param createdAtMs  when the value was loaded, epoch milliseconds
 * @param expiresAtMs  when the value stops being fresh; 0 if it never expires
 * @param staleUntilMs until when the expired value may still be served while it is refreshed; 0 if not at all
 */
public record CacheEntry(Object payload, long createdAtMs, long expiresAtMs, long staleUntilMs) {

    public CacheEntry {
        if (payload == null) {
            throw new IllegalArgumentException("A cache entry needs a payload; use KurinNullValue.INSTANCE for null");
        }
    }

    public boolean isNullMarker() {
        return payload instanceof KurinNullValue;
    }

    /**
     * @return the cached value as the caller should see it: null for the null marker
     */
    public Object value() {
        return isNullMarker() ? null : payload;
    }

    public boolean isFresh(long nowMs) {
        return expiresAtMs == 0 || nowMs < expiresAtMs;
    }

    /**
     * @return true if the value expired but is still inside its stale window
     */
    public boolean isStale(long nowMs) {
        return !isFresh(nowMs) && nowMs < staleUntilMs;
    }

    /**
     * @return the instant after which the entry is useless, or 0 if it never is
     */
    public long deadAtMs() {
        return Math.max(expiresAtMs, staleUntilMs);
    }

    public boolean isDead(long nowMs) {
        long deadAt = deadAtMs();
        return deadAt != 0 && nowMs >= deadAt;
    }
}
