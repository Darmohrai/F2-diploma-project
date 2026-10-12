package org.kurin.kurintieredcache.api;

/**
 * No cache key could be derived for a call: the key expression failed or produced null, or an argument has
 * a type the default key generator cannot render unambiguously.
 */
public class KurinCacheKeyException extends KurinCacheException {
    public KurinCacheKeyException(String message) {
        super(message);
    }

    public KurinCacheKeyException(String message, Throwable cause) {
        super(message, cause);
    }
}
