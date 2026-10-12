package org.kurin.kurintieredcache.api;

/**
 * What a {@link StampedeMode#CLUSTER} read does when it cannot get the cluster lock in time.
 */
public enum OnLockTimeout {
    /**
     * Load from the source anyway.
     */
    LOAD,
    /**
     * Throw {@code KurinCacheLockTimeoutException}.
     */
    FAIL,
    /**
     * Return an expired value if one is still stored, otherwise throw {@code KurinCacheLockTimeoutException}.
     */
    STALE
}
