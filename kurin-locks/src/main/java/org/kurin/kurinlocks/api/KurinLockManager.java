package org.kurin.kurinlocks.api;

import org.kurin.kurinlocks.command.AcquireLockCommand;
import org.kurin.kurinlocks.command.ReleaseLockCommand;
import org.kurin.raft.KurinNode;

import java.util.concurrent.TimeUnit;

public class KurinLockManager {

    private final KurinNode kurinNode;
    private final String localOwnerId;

    public KurinLockManager(KurinNode kurinNode) {
        this.kurinNode = kurinNode;
        this.localOwnerId = kurinNode.getLocalAddress().asString();
    }

    public boolean tryLock(String key, long ttlMs) {
        AcquireLockCommand cmd = new AcquireLockCommand(key, localOwnerId, ttlMs);
        try {
            Object result = kurinNode.submitCommand(cmd).get(3, TimeUnit.SECONDS);
            return Boolean.TRUE.equals(result);
        } catch (Exception e) {
            return false;
        }
    }

    public void unlock(String key) {
        ReleaseLockCommand cmd = new ReleaseLockCommand(key, localOwnerId);
        try {
            kurinNode.submitCommand(cmd).get(2, TimeUnit.SECONDS);
        } catch (Exception ignored) {
        }
    }
}
