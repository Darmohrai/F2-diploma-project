package org.kurin.kurinlocksspringbootstarter.aop;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocksspringbootstarter.annotation.KurinDistributedLock;
import org.kurin.raft.KurinNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.lang.reflect.Method;
import java.util.Arrays;

@Aspect
public class KurinLocksAspect {
    private static final Logger log = LoggerFactory.getLogger(KurinLocksAspect.class);

    private final KurinLockManager lockManager;
    private final KurinNode kurinNode;
    private final ExpressionParser parser = new SpelExpressionParser();

    public KurinLocksAspect(KurinLockManager lockManager, KurinNode kurinNode) {
        this.lockManager = lockManager;
        this.kurinNode = kurinNode;
    }

    @Around("@annotation(kurinLock)")
    public Object handleDistributedLock(ProceedingJoinPoint joinPoint, KurinDistributedLock kurinLock) throws Throwable {
        String key = parseSpelKey(joinPoint, kurinLock.key());
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

    private String parseSpelKey(ProceedingJoinPoint joinPoint, String spelExpression) {
        if (spelExpression == null || spelExpression.trim().isEmpty()) {
            return generateDefaultKey(joinPoint);
        }

        StandardEvaluationContext context = new StandardEvaluationContext();
        Object[] args = joinPoint.getArgs();
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        String[] paramNames = signature.getParameterNames();

        if (paramNames != null) {
            for (int i = 0; i < args.length; i++) {
                context.setVariable(paramNames[i], args[i]);
            }
        }

        try {
            return parser.parseExpression(spelExpression).getValue(context, String.class);
        } catch (Exception e) {
            return spelExpression;
        }
    }

    private String generateDefaultKey(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Object[] args = joinPoint.getArgs();
        return method.getName() + ":" + (args == null ? 0 : Arrays.hashCode(args));
    }
}
