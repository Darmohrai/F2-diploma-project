package org.kurin.kurincachespringbootstarter;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.kurin.kurintieredcache.api.KurinCacheable;
import org.kurin.kurintieredcache.core.KurinCache;
import org.kurin.kurintieredcache.core.KurinCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;

@Aspect
public class KurinCacheAspect {
    private static final Logger log = LoggerFactory.getLogger(KurinCacheAspect.class);
    private final KurinCacheManager cacheManager;

    public KurinCacheAspect(KurinCacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Around("@annotation(kurinCacheable)")
    public Object handleCacheable(ProceedingJoinPoint joinPoint, KurinCacheable kurinCacheable) throws Throwable {
        String cacheName = kurinCacheable.cacheName();
        KurinCache cache = cacheManager.getOrCreateCache(cacheName, kurinCacheable.tier());

        String key = kurinCacheable.key().isEmpty()
                ? generateDefaultKey(joinPoint)
                : kurinCacheable.key();

        Object cachedValue = cache.get(key);
        if (cachedValue != null) {
            log.debug("[Spring AOP] Cache HIT for key '{}' in cache '{}'", key, cacheName);
            return cachedValue;
        }

        log.debug("[Spring AOP] Cache MISS for key '{}'. Executing real method...", key);
        Object result = joinPoint.proceed();

        if (result != null) {
            cache.put(key, result);
        }
        return result;
    }

    private String generateDefaultKey(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Object[] args = joinPoint.getArgs();
        return method.getName() + ":" + (args == null ? 0 : Arrays.hashCode(args));
    }
}
