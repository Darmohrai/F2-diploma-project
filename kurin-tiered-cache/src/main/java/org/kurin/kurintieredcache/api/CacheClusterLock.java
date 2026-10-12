package org.kurin.kurintieredcache.api;

import java.time.Duration;
import java.util.Optional;

/**
 * Cluster-wide lock used by {@link StampedeMode#CLUSTER}. Implemented on top of the Kurin lock manager
 * when the locks module is present.
 */
public interface CacheClusterLock {

    /**
     * Tries to take the lock, waiting up to {@code waitTimeout}.
     *
     * @return a handle whose {@code close()} releases the lock, or empty if the lock was not obtained
     */
    Optional<Held> tryLock(String key, Duration ttl, Duration waitTimeout);

    interface Held extends AutoCloseable {
        @Override
        void close();
    }
}
