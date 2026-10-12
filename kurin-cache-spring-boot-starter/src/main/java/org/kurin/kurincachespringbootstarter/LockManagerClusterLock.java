package org.kurin.kurincachespringbootstarter;

import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurintieredcache.api.CacheClusterLock;

import java.time.Duration;
import java.util.Optional;

/**
 * Cluster lock of the cache on top of the Kurin lock manager. A waiting instance is woken by the release
 * event of the lock, so waiting writes nothing to the Raft log.
 */
public class LockManagerClusterLock implements CacheClusterLock {
    private final KurinLockManager lockManager;

    public LockManagerClusterLock(KurinLockManager lockManager) {
        this.lockManager = lockManager;
    }

    @Override
    public Optional<Held> tryLock(String key, Duration ttl, Duration waitTimeout) {
        return lockManager.tryLock(key, ttl, waitTimeout).map(handle -> handle::release);
    }
}
