package org.kurin.kurintieredcache.api;

import java.util.concurrent.CompletableFuture;

/**
 * Removes cache entries on every instance of the application.
 * <p>
 * Call it after the data changed in the source of truth (after the database transaction committed). The
 * calling instance first deletes the entries from shared tiers, then replicates the invalidation through
 * Raft; every instance drops the entries from its own tiers when it applies it. Reads that started loading
 * before the invalidation do not write their result back.
 */
public interface KurinCacheInvalidator {

    /**
     * @return a future completed once the invalidation is committed and applied on this instance. It fails with
     * a {@link org.kurin.api.exception.KurinException} if the cluster could not replicate it; the entries are
     * then removed on this instance and from shared tiers only.
     */
    CompletableFuture<Void> evict(String cacheName, String... keys);

    CompletableFuture<Void> evictAll(String cacheName);
}
