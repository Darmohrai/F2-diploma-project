package org.kurin.kurintieredcache.core;

import org.kurin.kurintieredcache.api.KurinCacheException;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Refuses values that must not be cached: persistence proxies and lazily loaded collections. They drag a
 * closed session along, serialize unpredictably and expose stale managed state. The check exists only when
 * Hibernate is on the classpath.
 */
public final class CacheValueGuard {
    private static final Class<?> HIBERNATE_PROXY = load("org.hibernate.proxy.HibernateProxy");
    private static final Class<?> PERSISTENT_COLLECTION = load("org.hibernate.collection.spi.PersistentCollection");
    private static final int MAX_ELEMENTS_CHECKED = 32;

    private CacheValueGuard() {
    }

    private static Class<?> load(String name) {
        try {
            return Class.forName(name, false, CacheValueGuard.class.getClassLoader());
        } catch (ClassNotFoundException | LinkageError e) {
            return null; // Hibernate is not used by this application
        }
    }

    /**
     * @throws KurinCacheException if the value, or an element directly inside it, is a managed persistence object
     */
    public static void check(String cacheName, Object value) {
        if (HIBERNATE_PROXY == null && PERSISTENT_COLLECTION == null) {
            return;
        }
        checkOne(cacheName, value);
        if (value instanceof Optional<?> optional && optional.isPresent()) {
            checkOne(cacheName, optional.get());
        } else if (value instanceof Collection<?> collection) {
            collection.stream().limit(MAX_ELEMENTS_CHECKED).forEach(element -> checkOne(cacheName, element));
        } else if (value instanceof Map<?, ?> map) {
            map.values().stream().limit(MAX_ELEMENTS_CHECKED).forEach(element -> checkOne(cacheName, element));
        }
    }

    private static void checkOne(String cacheName, Object value) {
        if (value == null) {
            return;
        }
        boolean proxy = HIBERNATE_PROXY != null && HIBERNATE_PROXY.isInstance(value);
        boolean lazyCollection = PERSISTENT_COLLECTION != null && PERSISTENT_COLLECTION.isInstance(value);
        if (proxy || lazyCollection) {
            throw new KurinCacheException("Cache '" + cacheName + "' was given a "
                    + (proxy ? "Hibernate proxy" : "Hibernate lazy collection") + " (" + value.getClass().getName()
                    + "). Cache DTOs, not managed entities: map the entity to a plain data object before returning it.");
        }
    }
}
