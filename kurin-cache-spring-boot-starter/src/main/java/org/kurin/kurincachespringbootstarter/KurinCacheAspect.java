package org.kurin.kurincachespringbootstarter;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurintieredcache.api.KurinCacheable;
import org.kurin.kurintieredcache.core.KurinCache;
import org.kurin.kurintieredcache.core.KurinCacheManager;
import org.kurin.kurintieredcache.core.KurinNullValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.lang.reflect.Method;
import java.util.Arrays;

@Aspect
public class KurinCacheAspect {
    private static final Logger log = LoggerFactory.getLogger(KurinCacheAspect.class);

    private final KurinCacheManager cacheManager;
    private final ObjectProvider<KurinLockManager> lockManagerProvider;
    private final ExpressionParser parser = new SpelExpressionParser();

    public KurinCacheAspect(KurinCacheManager cacheManager, ObjectProvider<KurinLockManager> lockManagerProvider) {
        this.cacheManager = cacheManager;
        this.lockManagerProvider = lockManagerProvider;
    }

    @Around("@annotation(kurinCacheable)")
    public Object handleCacheable(ProceedingJoinPoint joinPoint, KurinCacheable kurinCacheable) throws Throwable {
        String cacheName = kurinCacheable.cacheName();
        KurinCache cache = cacheManager.getOrCreateCache(cacheName, kurinCacheable.tier());

        String key = parseSpelKey(joinPoint, kurinCacheable.key());

        Object cachedValue = cache.get(key);
        if (cachedValue != null) {
            log.debug("[Spring AOP] Cache HIT for key '{}' in cache '{}'", key, cacheName);
            return unwrapNull(cachedValue);
        }

        if (!kurinCacheable.preventStampede()) {
            return proceedAndCache(joinPoint, cache, key, kurinCacheable);
        }

        KurinLockManager lockManager = lockManagerProvider.getIfAvailable();
        if (lockManager == null) {
            log.warn("[Spring AOP] preventStampede=true, but KurinLockManager is not available! Proceeding normally.");
            return proceedAndCache(joinPoint, cache, key, kurinCacheable);
        }

        String lockKey = "lock:stampede:" + cacheName + ":" + key;
        long ttlMs = kurinCacheable.stampedeLockTtlMs();
        // By this time the lock of the previous holder has expired, so waiting longer is pointless
        long waitDeadline = System.currentTimeMillis() + ttlMs;

        while (System.currentTimeMillis() < waitDeadline) {
            boolean acquired = lockManager.tryLock(lockKey, ttlMs);

            if (acquired) {
                try {
                    cachedValue = cache.get(key);
                    if (cachedValue != null) {
                        log.debug("[Spring AOP] Cache HIT (Double-Checked) for key '{}'", key);
                        return unwrapNull(cachedValue);
                    }

                    log.debug("[Spring AOP] Cache MISS. Fetching from DB with lock for key '{}'", key);
                    return proceedAndCache(joinPoint, cache, key, kurinCacheable);
                } finally {
                    lockManager.unlock(lockKey);
                }
            } else {
                log.debug("[Spring AOP] Lock occupied for key '{}', waiting...", key);
                try {
                    Thread.sleep(150);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for lock", e);
                }

                cachedValue = cache.get(key);
                if (cachedValue != null) {
                    return unwrapNull(cachedValue);
                }
            }
        }

        log.warn("[Spring AOP] Could not acquire stampede lock for key '{}' within {} ms. Proceeding without lock.", key, ttlMs);
        return proceedAndCache(joinPoint, cache, key, kurinCacheable);
    }

    private Object proceedAndCache(ProceedingJoinPoint joinPoint, KurinCache cache, String key, KurinCacheable config) throws Throwable {
        Object result = joinPoint.proceed();

        if (result != null) {
            putValueToCache(cache, key, result, config);
            return result;
        }

        if (config.cacheNulls()) {
            log.debug("[Spring AOP] DB returned null, caching KurinNullValue for key '{}'", key);
            putValueToCache(cache, key, KurinNullValue.INSTANCE, config);
        }

        return null;
    }

    private void putValueToCache(KurinCache cache, String key, Object value, KurinCacheable config) {
        int jitter = config.jitter();
        if (jitter > 0) {
            cache.putWithJitter(key, value, jitter);
        } else {
            cache.put(key, value);
        }
    }

    private Object unwrapNull(Object cachedValue) {
        if (cachedValue instanceof KurinNullValue || cachedValue == KurinNullValue.INSTANCE) {
            return null;
        }
        return cachedValue;
    }

    private String parseSpelKey(ProceedingJoinPoint joinPoint, String spelExpression) {
        if (spelExpression == null || spelExpression.trim().isEmpty()) {
            return generateDefaultKey(joinPoint);
        }

        StandardEvaluationContext context = new StandardEvaluationContext();
        Object[] args = joinPoint.getArgs();
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        String[] paramNames = signature.getParameterNames();

        // Positional aliases work even when the code is compiled without -parameters
        for (int i = 0; i < args.length; i++) {
            context.setVariable("a" + i, args[i]);
            context.setVariable("p" + i, args[i]);
            if (paramNames != null) {
                context.setVariable(paramNames[i], args[i]);
            }
        }

        String key;
        try {
            key = parser.parseExpression(spelExpression).getValue(context, String.class);
        } catch (Exception e) {
            return spelExpression;
        }

        if (key == null) {
            log.warn("[Spring AOP] Key expression '{}' evaluated to null for method {} (compiled without -parameters?). Using default key.",
                    spelExpression, signature.getName());
            return generateDefaultKey(joinPoint);
        }
        return key;
    }

    private String generateDefaultKey(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Object[] args = joinPoint.getArgs();
        return method.getName() + ":" + (args == null ? 0 : Arrays.hashCode(args));
    }
}
