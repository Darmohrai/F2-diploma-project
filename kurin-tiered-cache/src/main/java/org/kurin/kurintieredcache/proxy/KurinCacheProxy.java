package org.kurin.kurintieredcache.proxy;

import org.kurin.kurintieredcache.api.KurinCacheable;
import org.kurin.kurintieredcache.core.KurinCache;
import org.kurin.kurintieredcache.core.KurinCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;

public class KurinCacheProxy implements InvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(KurinCacheProxy.class);

    private final Object target;
    private final KurinCacheManager cacheManager;

    private KurinCacheProxy(Object target, KurinCacheManager cacheManager) {
        this.target = target;
        this.cacheManager = cacheManager;
    }

    @SuppressWarnings("unchecked")
    public static <T> T wrap(T target, Class<T> interfaceType, KurinCacheManager cacheManager) {
        return (T) Proxy.newProxyInstance(
                interfaceType.getClassLoader(),
                new Class<?>[]{interfaceType},
                new KurinCacheProxy(target, cacheManager)
        );
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Method targetMethod = target.getClass().getMethod(method.getName(), method.getParameterTypes());

        if (targetMethod.isAnnotationPresent(KurinCacheable.class)) {
            KurinCacheable cacheable = targetMethod.getAnnotation(KurinCacheable.class);
            KurinCache cache = cacheManager.getOrCreateCache(cacheable.cacheName(), cacheable.tier());

            String key = cacheable.key().isEmpty()
                    ? method.getName() + ":" + (args == null ? 0 : Arrays.hashCode(args))
                    : cacheable.key();

            Object cachedValue = cache.get(key);
            if (cachedValue != null) {
                log.debug("[Proxy] Cache HIT for key '{}' in tier '{}'", key, cacheable.tier());
                return cachedValue;
            }

            log.debug("[Proxy] Cache MISS for key '{}'. Executing real method...", key);
            Object result = method.invoke(target, args);

            if (result != null) {
                cache.put(key, result);
            }
            return result;
        }

        return method.invoke(target, args);
    }
}
