package org.kurin.kurinlocks.state;

import org.kurin.api.KurinCommand;
import org.kurin.kurinlocks.command.AcquireLockCommand;
import org.kurin.kurinlocks.command.ReleaseLockCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DistributedLockService {
    private static final Logger log = LoggerFactory.getLogger(DistributedLockService.class);

    private final Map<String, LockInfo> locks = new HashMap<>();

    public record LockInfo(String ownerId, long expiresAtMs) {
        public boolean isExpired(long now) {
            return now > expiresAtMs;
        }
    }

    @KurinCommand
    public Boolean acquireLock(AcquireLockCommand cmd) {
        long now = System.currentTimeMillis();
        LockInfo current = locks.get(cmd.lockKey());

        if (current == null || current.isExpired(now)) {
            locks.put(cmd.lockKey(), new LockInfo(cmd.ownerId(), now + cmd.ttlMs()));
            log.debug("[Kurin Locks] Вузол {} успішно захопив лок '{}'", cmd.ownerId(), cmd.lockKey());
            return true;
        }

        if (current.ownerId().equals(cmd.ownerId())) {
            locks.put(cmd.lockKey(), new LockInfo(cmd.ownerId(), now + cmd.ttlMs()));
            log.debug("[Kurin Locks] Вузол {} подовжив лок '{}'", cmd.ownerId(), cmd.lockKey());
            return true;
        }

        log.debug("[Kurin Locks] Відмова для {}. Лок '{}' утримується вузлом {}", cmd.ownerId(), cmd.lockKey(), current.ownerId());
        return false;
    }

    @KurinCommand
    public Boolean releaseLock(ReleaseLockCommand cmd) {
        LockInfo current = locks.get(cmd.lockKey());
        if (current != null && current.ownerId().equals(cmd.ownerId())) {
            locks.remove(cmd.lockKey());
            log.debug("[Kurin Locks] Вузол {} успішно звільнив лок '{}'", cmd.ownerId(), cmd.lockKey());
            return true;
        }
        return false;
    }
}
