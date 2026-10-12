package org.kurin.kurincachespringbootstarter;

import org.kurin.kurintieredcache.api.CacheKeyProvider;
import org.kurin.kurintieredcache.api.KeyGenerator;
import org.kurin.kurintieredcache.api.KurinCacheEvict;
import org.kurin.kurintieredcache.api.KurinCacheKeyException;
import org.kurin.kurintieredcache.api.KurinCachePut;
import org.kurin.kurintieredcache.core.CacheSettings;
import org.kurin.kurintieredcache.core.KurinCacheManager;
import org.springframework.boot.convert.DurationStyle;

import java.lang.reflect.Method;
import java.time.Duration;

/**
 * What the cache aspect and the command listener share: resolving keys and effective settings.
 */
public class CacheAnnotationSupport {
    private final KurinCacheManager manager;
    private final CacheExpressions expressions;
    private final KeyGenerator keyGenerator;

    public CacheAnnotationSupport(KurinCacheManager manager, CacheExpressions expressions, KeyGenerator keyGenerator) {
        this.manager = manager;
        this.expressions = expressions;
        this.keyGenerator = keyGenerator;
    }

    public KurinCacheManager manager() {
        return manager;
    }

    public CacheExpressions expressions() {
        return expressions;
    }

    /**
     * Key of a read: the expression if there is one, otherwise the generated key of the call.
     */
    public String readKey(String expression, Method method, Object target, Object[] arguments) {
        if (!expression.isEmpty()) {
            return expressions.key(expression, method, target, arguments, CacheExpressions.NO_RESULT);
        }
        return keyGenerator.generate(target, method, arguments);
    }

    /**
     * Key of a write or an eviction: the expression if there is one, otherwise the key named by the first
     * argument as a {@link CacheKeyProvider}.
     */
    public String writeKey(String expression, Method method, Object target, Object[] arguments, Object result) {
        if (!expression.isEmpty()) {
            return expressions.key(expression, method, target, arguments, result);
        }
        if (arguments.length > 0 && arguments[0] instanceof CacheKeyProvider provider) {
            String key = provider.getCacheKey();
            if (key == null) {
                throw new KurinCacheKeyException(arguments[0].getClass().getName() + ".getCacheKey() returned null in "
                        + CacheExpressions.describe(method));
            }
            return key;
        }
        throw new KurinCacheKeyException(CacheExpressions.describe(method) + " has no cache key: give the annotation a key "
                + "expression or let the first argument implement CacheKeyProvider");
    }

    public CacheSettings settings(String cacheName, String ttl) {
        CacheSettings settings = manager.settingsOf(cacheName);
        return ttl.isEmpty() ? settings : settings.withTtl(parseDuration(ttl));
    }

    public static Duration parseDuration(String value) {
        return DurationStyle.detectAndParse(value);
    }

    /**
     * @return true if the method may leave the key out: it evicts everything, or its first parameter names the key
     */
    public static boolean keyIsOptional(Method method, KurinCachePut put, KurinCacheEvict evict) {
        boolean firstParameterNamesKey = method.getParameterCount() > 0
                && CacheKeyProvider.class.isAssignableFrom(method.getParameterTypes()[0]);
        return firstParameterNamesKey || (put == null && evict != null && evict.allEntries());
    }
}
