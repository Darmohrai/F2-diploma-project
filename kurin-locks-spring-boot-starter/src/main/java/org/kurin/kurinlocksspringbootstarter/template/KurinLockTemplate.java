package org.kurin.kurinlocksspringbootstarter.template;

import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocks.api.LockHandle;
import org.kurin.kurinlocks.api.LockOptions;
import org.kurin.kurinlocks.exception.KurinLockAcquisitionException;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Programmatic counterpart of {@code @KurinDistributedLock}: runs a piece of code under a cluster-wide lock
 * and always releases it.
 */
public class KurinLockTemplate {
    private final KurinLockManager lockManager;

    public KurinLockTemplate(KurinLockManager lockManager) {
        this.lockManager = lockManager;
    }

    /**
     * Runs the action under the lock.
     *
     * @throws KurinLockAcquisitionException if the lock is busy
     */
    public void executeWithLock(String key, Duration ttl, Runnable action) {
        executeWithLock(key, LockOptions.ttl(ttl), handle -> {
            action.run();
            return null;
        });
    }

    public <T> T executeWithLock(String key, Duration ttl, Supplier<T> action) {
        return executeWithLock(key, LockOptions.ttl(ttl), handle -> action.get());
    }

    /**
     * Runs the action under the lock, waiting up to {@code waitTimeout} for a busy lock.
     *
     * @throws KurinLockAcquisitionException if the lock was not obtained in time
     */
    public <T> T executeWithLock(String key, Duration ttl, Duration waitTimeout, Supplier<T> action) {
        return executeWithLock(key, LockOptions.ttl(ttl).waitTimeout(waitTimeout), handle -> action.get());
    }

    /**
     * @param action receives the handle, for example to read the fencing token or to renew the lock
     */
    public <T> T executeWithLock(String key, LockOptions options, Function<LockHandle, T> action) {
        try (LockHandle handle = lockManager.lock(key, options)) {
            return action.apply(handle);
        }
    }

    /**
     * Runs the action if the lock is free.
     *
     * @return the result of the action, or empty if the lock is busy and the action did not run
     */
    public <T> Optional<T> tryExecuteWithLock(String key, Duration ttl, Supplier<T> action) {
        Optional<LockHandle> acquired = lockManager.tryLock(key, ttl);
        if (acquired.isEmpty()) {
            return Optional.empty();
        }
        try (LockHandle handle = acquired.get()) {
            return Optional.ofNullable(action.get());
        }
    }
}
