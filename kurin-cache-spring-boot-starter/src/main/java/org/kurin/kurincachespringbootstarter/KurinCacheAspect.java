package org.kurin.kurincachespringbootstarter;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.kurin.api.KurinCommand;
import org.kurin.api.KurinInstrumentation;
import org.kurin.raft.KurinNode;
import org.kurin.kurintieredcache.api.KurinCacheEvict;
import org.kurin.kurintieredcache.api.KurinCacheInvalidator;
import org.kurin.kurintieredcache.api.KurinCachePut;
import org.kurin.kurintieredcache.api.KurinCacheable;
import org.kurin.kurintieredcache.core.CacheReader;
import org.kurin.kurintieredcache.core.CacheReader.ReadOptions;
import org.kurin.kurintieredcache.core.CacheSettings;
import org.kurin.kurintieredcache.core.CacheValueGuard;
import org.kurin.kurintieredcache.core.TieredCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.Order;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * Applies the cache annotations to ordinary bean methods. On {@code @KurinCommand} handlers
 * {@code @KurinCachePut} and {@code @KurinCacheEvict} are applied by {@link CommandCacheListener} instead,
 * when the command is applied on each instance.
 * <p>
 * The aspect runs outside of the transaction advice. By the time an advised transactional method returns
 * here, its transaction has committed, and if it rolled back an exception passes through instead.
 */
@Aspect
@Order(Ordered.HIGHEST_PRECEDENCE + 300)
public class KurinCacheAspect {
    private static final Logger log = LoggerFactory.getLogger(KurinCacheAspect.class);
    private static final boolean TRANSACTIONS_PRESENT = ClassUtils.isPresent(
            "org.springframework.transaction.support.TransactionSynchronizationManager", KurinCacheAspect.class.getClassLoader());

    /**
     * Everything about a {@code @KurinCacheable} method that does not change between calls.
     */
    private record CacheablePlan(String cacheName, List<org.kurin.kurintieredcache.api.CacheTier> tiers, CacheSettings settings,
                                 Duration lockWait) {
    }

    private final CacheAnnotationSupport support;
    private final CacheReader reader;
    private final KurinCacheInvalidator invalidator;
    private final Map<Method, CacheablePlan> cacheablePlans = new ConcurrentHashMap<>();

    private final KurinNode node;

    public KurinCacheAspect(CacheAnnotationSupport support, CacheReader reader, KurinCacheInvalidator invalidator, KurinNode node) {
        this.node = node;
        this.support = support;
        this.reader = reader;
        this.invalidator = invalidator;
    }

    // ---------------------------------------------------------------- @KurinCacheable

    @Around("@annotation(cacheable)")
    public Object handleCacheable(ProceedingJoinPoint joinPoint, KurinCacheable cacheable) throws Throwable {
        Method method = targetMethod(joinPoint);
        Object target = joinPoint.getTarget();
        Object[] arguments = joinPoint.getArgs();

        if (!cacheable.condition().isEmpty()
                && !support.expressions().condition(cacheable.condition(), method, target, arguments, CacheExpressions.NO_RESULT)) {
            return joinPoint.proceed();
        }

        CacheablePlan plan = cacheablePlans.computeIfAbsent(method, m -> cacheablePlan(cacheable));
        TieredCache cache = support.manager().getCache(plan.cacheName(), plan.tiers());
        String key = support.readKey(cacheable.key(), method, target, arguments);

        Predicate<Object> unless = cacheable.unless().isEmpty() ? null
                : result -> support.expressions().condition(cacheable.unless(), method, target, arguments, result);
        ReadOptions options = new ReadOptions(plan.settings(), cacheable.cacheNulls(), cacheable.stampede(), plan.lockWait(),
                cacheable.onLockTimeout(), unless);

        KurinInstrumentation.Span span = node.startSpan("kurin.cache.get", Map.of("cache", plan.cacheName()));
        Throwable failure = null;
        try {
            return reader.read(cache, key, options, () -> {
                KurinInstrumentation.Span load = node.startSpan("kurin.cache.load", Map.of("cache", plan.cacheName()));
                try {
                    Object loaded = proceedWrapping(joinPoint);
                    load.end(null);
                    return loaded;
                } catch (Exception | Error e) {
                    load.end(e);
                    throw e;
                }
            }, result -> {
                span.tag("result", result.name());
                span.tag("tier", switch (result) {
                    case HIT_L1 -> "L1";
                    case HIT_L2 -> "L2";
                    case HIT_L3 -> "L3";
                    case STALE, MISS -> "none";
                });
            });
        } catch (UndeclaredThrowableException e) {
            failure = e.getUndeclaredThrowable();
            throw failure;
        } catch (Exception | Error e) {
            failure = e;
            throw e;
        } finally {
            span.end(failure);
        }
    }

    private CacheablePlan cacheablePlan(KurinCacheable cacheable) {
        CacheSettings settings = support.manager().settingsOf(cacheable.cacheName());
        if (!cacheable.ttl().isEmpty()) {
            settings = settings.withTtl(CacheAnnotationSupport.parseDuration(cacheable.ttl()));
        }
        if (!cacheable.nullTtl().isEmpty()) {
            settings = settings.withNullTtl(CacheAnnotationSupport.parseDuration(cacheable.nullTtl()));
        }
        if (cacheable.jitterPercent() >= 0) {
            settings = settings.withJitterPercent(cacheable.jitterPercent());
        }
        if (!cacheable.staleWhileRevalidate().isEmpty()) {
            settings = settings.withStaleWhileRevalidate(CacheAnnotationSupport.parseDuration(cacheable.staleWhileRevalidate()));
        }
        return new CacheablePlan(cacheable.cacheName(), Arrays.asList(cacheable.tiers()), settings,
                CacheAnnotationSupport.parseDuration(cacheable.lockWait()));
    }

    // ---------------------------------------------------------------- @KurinCachePut

    @Around("@annotation(cachePut)")
    public Object handleCachePut(ProceedingJoinPoint joinPoint, KurinCachePut cachePut) throws Throwable {
        Method method = targetMethod(joinPoint);
        Object result = joinPoint.proceed();
        if (isCommandHandler(method) || result == null) {
            return result;
        }
        try {
            String key = support.writeKey(cachePut.key(), method, joinPoint.getTarget(), joinPoint.getArgs(), result);
            CacheValueGuard.check(cachePut.cacheName(), result);
            CacheSettings settings = support.settings(cachePut.cacheName(), cachePut.ttl());
            support.manager().getCache(cachePut.cacheName()).put(key, settings.newEntry(result, System.currentTimeMillis()));
        } catch (RuntimeException e) {
            // The method has done its work; a cache that was not updated must not turn that into a failure
            log.warn("Result of {} was not cached: {}", CacheExpressions.describe(method), e.toString());
        }
        return result;
    }

    // ---------------------------------------------------------------- @KurinCacheEvict

    @Around("@annotation(cacheEvict)")
    public Object handleCacheEvict(ProceedingJoinPoint joinPoint, KurinCacheEvict cacheEvict) throws Throwable {
        Method method = targetMethod(joinPoint);
        Object result = joinPoint.proceed();
        if (isCommandHandler(method)) {
            return result;
        }

        String key = cacheEvict.allEntries() ? null
                : support.writeKey(cacheEvict.key(), method, joinPoint.getTarget(), joinPoint.getArgs(), result);
        Runnable eviction = () -> evict(cacheEvict, key, method);

        if (TRANSACTIONS_PRESENT && AfterCommit.isTransactionActive()) {
            // Called inside a surrounding transaction: its outcome is not known yet
            AfterCommit.run(eviction);
        } else {
            eviction.run();
        }
        return result;
    }

    private void evict(KurinCacheEvict cacheEvict, String key, Method method) {
        for (String cacheName : cacheEvict.cacheNames()) {
            try {
                if (cacheEvict.replicate()) {
                    (key == null ? invalidator.evictAll(cacheName) : invalidator.evict(cacheName, key))
                            .whenComplete((done, error) -> {
                                if (error != null) {
                                    log.warn("Invalidation of cache '{}' after {} was not replicated to all instances: {}",
                                            cacheName, CacheExpressions.describe(method), error.toString());
                                }
                            });
                } else if (key == null) {
                    support.manager().getCache(cacheName).clear();
                } else {
                    support.manager().getCache(cacheName).evict(key);
                }
            } catch (RuntimeException e) {
                log.error("Cache '{}' could not be invalidated after {}", cacheName, CacheExpressions.describe(method), e);
            }
        }
    }

    /**
     * Kept apart so that the transaction classes are touched only when spring-tx is on the classpath.
     */
    private static final class AfterCommit {
        static boolean isTransactionActive() {
            return org.springframework.transaction.support.TransactionSynchronizationManager.isSynchronizationActive()
                    && org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive();
        }

        static void run(Runnable action) {
            org.springframework.transaction.support.TransactionSynchronizationManager.registerSynchronization(
                    new org.springframework.transaction.support.TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            action.run();
                        }
                    });
        }
    }

    // ---------------------------------------------------------------- helpers

    private static Method targetMethod(ProceedingJoinPoint joinPoint) {
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        return joinPoint.getTarget() != null ? AopUtils.getMostSpecificMethod(method, joinPoint.getTarget().getClass()) : method;
    }

    private static boolean isCommandHandler(Method method) {
        return AnnotatedElementUtils.hasAnnotation(method, KurinCommand.class);
    }

    // A checked Throwable of the advised method crosses the Callable-based reader wrapped, and is unwrapped by the caller
    private static Object proceedWrapping(ProceedingJoinPoint joinPoint) throws Exception {
        try {
            return joinPoint.proceed();
        } catch (Exception | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new UndeclaredThrowableException(t);
        }
    }
}
