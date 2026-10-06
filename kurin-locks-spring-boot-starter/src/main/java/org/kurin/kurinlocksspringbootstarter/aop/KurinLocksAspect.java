package org.kurin.kurinlocksspringbootstarter.aop;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinDistributedLock;
import org.kurin.raft.KurinNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class KurinLocksAspect {
    private static final Logger log = LoggerFactory.getLogger(KurinLocksAspect.class);

    private final KurinLockManager lockManager;
    private final KurinNode kurinNode;

    public KurinLocksAspect(KurinLockManager lockManager, KurinNode kurinNode) {
        this.lockManager = lockManager;
        this.kurinNode = kurinNode;
    }

    @Around("@annotation(kurinLock)")
    public Object handleDistributedLock(ProceedingJoinPoint joinPoint, KurinDistributedLock kurinLock) throws Throwable {
        String key = kurinLock.key();
        boolean acquired = lockManager.tryLock(key, kurinLock.ttlMs());

        if (!acquired) {
            log.warn("[Kurin Locks] Лок '{}' зайнятий. Метод {} не буде виконано на цій ноді.",
                    key, joinPoint.getSignature().getName());
            return null;
        }

        try {
            return joinPoint.proceed();
        } finally {
            lockManager.unlock(key);
        }
    }

    @Around("@annotation(org.kurin.kurinlocksspringbootstarter.annotation.KurinLeaderOnly)")
    public Object handleLeaderOnly(ProceedingJoinPoint joinPoint) throws Throwable {
        if (kurinNode.isLeader()) {
            log.debug("[LeaderOnly] Виконуємо задачу {} (Я - лідер)", joinPoint.getSignature().getName());
            return joinPoint.proceed();
        } else {
            log.trace("[LeaderOnly] Пропуск задачі {}. Цей вузол не є лідером.", joinPoint.getSignature().getName());
            return null;
        }
    }
}
