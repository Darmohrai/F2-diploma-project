package org.kurin.kurintieredcache.api;

/**
 * A read protected by a cluster lock did not get the lock in time and was configured not to load without it.
 */
public class KurinCacheLockTimeoutException extends KurinCacheException {
    public KurinCacheLockTimeoutException(String message) {
        super(message);
    }
}
