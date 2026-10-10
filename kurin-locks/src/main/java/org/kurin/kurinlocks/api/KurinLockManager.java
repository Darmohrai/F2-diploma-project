package org.kurin.kurinlocks.api;

import org.kurin.kurinlocks.command.AcquireLockCommand;
import org.kurin.kurinlocks.command.ReleaseLockCommand;
import org.kurin.raft.KurinNode;

import java.util.concurrent.TimeUnit;

public class KurinLockManager {
    private final KurinNode kurinNode;
    private final String localOwnerId;
    private final long lockTimeoutMs;
    private final long unlockTimeoutMs;

    public KurinLockManager(KurinNode kurinNode, long lockTimeoutMs, long unlockTimeoutMs) {
        this.kurinNode = kurinNode;
        this.localOwnerId = kurinNode.getLocalAddress().asString();
        this.lockTimeoutMs = lockTimeoutMs;
        this.unlockTimeoutMs = unlockTimeoutMs;
    }

    public boolean tryLock(String key, long ttlMs) {
        AcquireLockCommand cmd = new AcquireLockCommand(key, localOwnerId, ttlMs);
        try {
            Object result = kurinNode.submitCommand(cmd).get(lockTimeoutMs, TimeUnit.MILLISECONDS);
            return Boolean.TRUE.equals(result);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    public void unlock(String key) {
        ReleaseLockCommand cmd = new ReleaseLockCommand(key, localOwnerId);
        try {
            kurinNode.submitCommand(cmd).get(unlockTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
        }
    }
}
