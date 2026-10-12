package org.kurin.kurintieredcache.key;

import org.kurin.kurintieredcache.api.KeyGenerator;
import org.kurin.kurintieredcache.api.KurinCacheKeyException;

import java.lang.reflect.Method;

/**
 * Builds keys of the form {@code ClassSimpleName.method(arg1,arg2,...)} with every argument rendered by
 * {@link CacheKeyRenderer}. Unlike a hash of the arguments, two different calls never share a key.
 */
public class DefaultKeyGenerator implements KeyGenerator {

    @Override
    public String generate(Object target, Method method, Object[] arguments) {
        StringBuilder key = new StringBuilder(64);
        key.append(method.getDeclaringClass().getSimpleName()).append('.').append(method.getName()).append('(');
        try {
            for (int i = 0; arguments != null && i < arguments.length; i++) {
                if (i > 0) {
                    key.append(',');
                }
                CacheKeyRenderer.append(key, arguments[i]);
            }
        } catch (KurinCacheKeyException e) {
            throw new KurinCacheKeyException("No default cache key for " + method.getDeclaringClass().getName() + "."
                    + method.getName() + ": " + e.getMessage(), e);
        }
        return key.append(')').toString();
    }
}
