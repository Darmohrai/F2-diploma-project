package org.kurin.kurintieredcache.api;

/**
 * How concurrent misses of the same key are handled.
 */
public enum StampedeMode {
    /**
     * Every caller loads on its own.
     */
    NONE,
    /**
     * Single flight per instance: concurrent callers on one instance share one load.
     */
    LOCAL,
    /**
     * Single flight per instance, plus a cluster-wide lock: one load for the whole cluster, provided the cache
     * has a shared tier through which the other instances can see the result.
     */
    CLUSTER
}
