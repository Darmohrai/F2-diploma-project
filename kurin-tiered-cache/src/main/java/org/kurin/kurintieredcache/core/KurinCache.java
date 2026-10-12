package org.kurin.kurintieredcache.core;

public interface KurinCache extends AutoCloseable {
    String getName();

    Object get(String key);

    void put(String key, Object value);

    default void putWithJitter(String key, Object value, int jitterPercentage) {
        put(key, value);
    }

    void evict(String key);

    void clear();

    @Override
    void close();
}
