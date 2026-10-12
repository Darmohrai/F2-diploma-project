package org.kurin.kurincachespringbootstarter;

import org.kurin.api.KurinCommand;
import org.kurin.api.KurinCommandContext;
import org.kurin.kurintieredcache.api.KurinCacheEvict;
import org.kurin.kurintieredcache.api.KurinCachePut;
import org.kurin.kurintieredcache.core.CacheSettings;
import org.kurin.kurintieredcache.core.CacheValueGuard;
import org.kurin.kurintieredcache.core.TieredCache;
import org.kurin.raft.state.StateMachineListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies {@code @KurinCachePut} and {@code @KurinCacheEvict} of {@code @KurinCommand} handlers. It is called
 * on every instance after the command was applied there, so each instance updates its own tiers.
 */
public class CommandCacheListener implements StateMachineListener {
    private static final Logger log = LoggerFactory.getLogger(CommandCacheListener.class);

    private record Handler(Object target, Method method, KurinCachePut put, KurinCacheEvict evict) {
    }

    private final CacheAnnotationSupport support;
    private final Map<Class<?>, Handler> handlers = new HashMap<>();

    public CommandCacheListener(CacheAnnotationSupport support, List<Object> services) {
        this.support = support;
        for (Object service : services) {
            ReflectionUtils.doWithMethods(AopUtils.getTargetClass(service), method -> {
                KurinCachePut put = AnnotatedElementUtils.findMergedAnnotation(method, KurinCachePut.class);
                KurinCacheEvict evict = AnnotatedElementUtils.findMergedAnnotation(method, KurinCacheEvict.class);
                if ((put != null || evict != null) && method.getParameterCount() == 1) {
                    handlers.putIfAbsent(method.getParameterTypes()[0], new Handler(service, method, put, evict));
                }
            }, method -> AnnotatedElementUtils.hasAnnotation(method, KurinCommand.class) && !method.isBridge());
        }
        log.debug("Cache annotations found on {} command handler(s)", handlers.size());
    }

    @Override
    public void onCommandApplied(Object command, Object result) {
        Handler handler = handlers.get(command.getClass());
        if (handler == null) {
            return;
        }
        Object[] arguments = {command};

        if (handler.put() != null && result != null) {
            KurinCachePut put = handler.put();
            String key = support.writeKey(put.key(), handler.method(), handler.target(), arguments, result);
            CacheValueGuard.check(put.cacheName(), result);
            CacheSettings settings = support.settings(put.cacheName(), put.ttl());
            // The time of the command, not of this instance: every instance creates the same entry
            long now = KurinCommandContext.isActive() ? KurinCommandContext.current().timestampMs() : System.currentTimeMillis();
            support.manager().getCache(put.cacheName()).put(key, settings.newEntry(result, now));
            log.debug("Command {} updated '{}' in cache '{}'", command.getClass().getSimpleName(), key, put.cacheName());
        }

        if (handler.evict() != null) {
            KurinCacheEvict evict = handler.evict();
            String key = evict.allEntries() ? null
                    : support.writeKey(evict.key(), handler.method(), handler.target(), arguments, result);
            for (String cacheName : evict.cacheNames()) {
                TieredCache cache = support.manager().getCache(cacheName);
                // Every instance applies the command, so every instance cleans its own tiers; deleting from a
                // shared tier more than once is harmless
                if (key == null) {
                    cache.clearShared();
                    cache.clearLocal("command");
                } else {
                    cache.evictShared(key);
                    cache.evictLocal(key, "command");
                }
            }
        }
    }
}
