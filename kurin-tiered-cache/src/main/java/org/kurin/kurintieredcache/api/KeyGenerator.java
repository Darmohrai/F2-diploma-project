package org.kurin.kurintieredcache.api;

import java.lang.reflect.Method;

/**
 * Derives the cache key of a call when the annotation gives no key expression.
 */
@FunctionalInterface
public interface KeyGenerator {

    /**
     * @return a key that is equal for equal arguments and different for different ones
     * @throws KurinCacheKeyException if no unambiguous key can be derived
     */
    String generate(Object target, Method method, Object[] arguments);
}
