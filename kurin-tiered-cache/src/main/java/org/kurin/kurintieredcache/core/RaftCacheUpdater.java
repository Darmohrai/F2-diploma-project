package org.kurin.kurintieredcache.core;

import org.kurin.kurintieredcache.api.CacheKeyProvider;
import org.kurin.kurintieredcache.api.KurinCacheEvict;
import org.kurin.kurintieredcache.api.KurinCachePut;
import org.kurin.raft.state.StateMachineListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RaftCacheUpdater implements StateMachineListener {
    private static final Logger log = LoggerFactory.getLogger(RaftCacheUpdater.class);

    private final KurinCacheManager cacheManager;
    private final Map<Class<?>, KurinCachePut> putHandlers = new HashMap<>();
    private final Map<Class<?>, KurinCacheEvict> evictHandlers = new HashMap<>();

    public RaftCacheUpdater(KurinCacheManager cacheManager, List<Object> services) {
        this.cacheManager = cacheManager;

        for (Object service : services) {
            for (Method method : service.getClass().getDeclaredMethods()) {
                if (method.getParameterCount() != 1) continue;

                KurinCachePut putAnnotation = findAnnotation(method, KurinCachePut.class);
                if (putAnnotation != null) {
                    putHandlers.put(method.getParameterTypes()[0], putAnnotation);
                }

                KurinCacheEvict evictAnnotation = findAnnotation(method, KurinCacheEvict.class);
                if (evictAnnotation != null) {
                    evictHandlers.put(method.getParameterTypes()[0], evictAnnotation);
                }
            }
        }
        log.info("[RaftCacheUpdater] Registered cache update methods: PUT={}, EVICT={}",
                putHandlers.size(), evictHandlers.size());
    }

    @Override
    public void onCommandApplied(Object command, Object result) {
        if (command == null) return;

        Class<?> cmdClass = command.getClass();

        KurinCachePut putMeta = putHandlers.get(cmdClass);
        if (putMeta != null && result != null) {
            String key = resolveKey(command, putMeta.key());
            KurinCache cache = cacheManager.getOrCreateCache(putMeta.cacheName(), putMeta.tier());
            cache.put(key, result);
            log.debug("[RaftCacheUpdater] Updated cache '{}' (Tier: {}) for key '{}'",
                    putMeta.cacheName(), putMeta.tier(), key);
        }

        KurinCacheEvict evictMeta = evictHandlers.get(cmdClass);
        if (evictMeta != null) {
            KurinCache cache = cacheManager.getCache(evictMeta.cacheName());
            if (cache != null) {
                if (evictMeta.allEntries()) {
                    cache.clear();
                    log.debug("[RaftCacheUpdater] Cleared entire cache '{}'", evictMeta.cacheName());
                } else {
                    String key = resolveKey(command, evictMeta.key());
                    cache.evict(key);
                    log.debug("[RaftCacheUpdater] Evicted key '{}' from cache '{}'", key, evictMeta.cacheName());
                }
            }
        }
    }

    private String resolveKey(Object command, String defaultKey) {
        if (defaultKey != null && !defaultKey.isEmpty()) {
            return defaultKey;
        }
        if (command instanceof CacheKeyProvider provider) {
            return provider.getCacheKey();
        }
        return command.toString();
    }

    private <T extends Annotation> T findAnnotation(Method method, Class<T> annotationClass) {
        if (method.isAnnotationPresent(annotationClass)) {
            return method.getAnnotation(annotationClass);
        }

        Class<?> declaringClass = method.getDeclaringClass();

        Class<?> superclass = declaringClass.getSuperclass();
        while (superclass != null && superclass != Object.class) {
            try {
                Method superMethod = superclass.getDeclaredMethod(method.getName(), method.getParameterTypes());
                if (superMethod.isAnnotationPresent(annotationClass)) {
                    return superMethod.getAnnotation(annotationClass);
                }
            } catch (NoSuchMethodException ignored) {
            }
            superclass = superclass.getSuperclass();
        }

        for (Class<?> iface : declaringClass.getInterfaces()) {
            try {
                Method ifaceMethod = iface.getMethod(method.getName(), method.getParameterTypes());
                if (ifaceMethod.isAnnotationPresent(annotationClass)) {
                    return ifaceMethod.getAnnotation(annotationClass);
                }
            } catch (NoSuchMethodException ignored) {
            }
        }

        return null;
    }
}
