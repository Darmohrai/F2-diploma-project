package org.kurin.kurinlocksspringbootstarter.template;

import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocks.exception.KurinLockAcquisitionException;

import java.util.function.Supplier;

public class KurinLockTemplate {

    private final KurinLockManager lockManager;

    public KurinLockTemplate(KurinLockManager lockManager) {
        this.lockManager = lockManager;
    }

    public void executeWithLock(String key, long ttlMs, Runnable action) {
        if (lockManager.tryLock(key, ttlMs)) {
            try {
                action.run();
            } finally {
                lockManager.unlock(key);
            }
        } else {
            throw new KurinLockAcquisitionException("Failed to acquire distributed lock for key: " + key);
        }
    }

    public <T> T executeWithLock(String key, long ttlMs, Supplier<T> action) {
        if (lockManager.tryLock(key, ttlMs)) {
            try {
                return action.get();
            } finally {
                lockManager.unlock(key);
            }
        } else {
            throw new KurinLockAcquisitionException("Failed to acquire distributed lock for key: " + key);
        }
    }
}
