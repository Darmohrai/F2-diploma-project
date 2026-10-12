package org.kurin.kurintieredcache.core;

public record CacheEntry(Object payload, long expiresAtMs) {
    public boolean isExpired() {
        return expiresAtMs > 0 && System.currentTimeMillis() > expiresAtMs;
    }
}
