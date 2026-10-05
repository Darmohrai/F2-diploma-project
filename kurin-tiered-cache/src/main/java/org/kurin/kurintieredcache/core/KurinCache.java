package org.kurin.kurintieredcache.core;

public interface KurinCache extends AutoCloseable {
    String getName();

    Object get(String key);

    void put(String key, Object value);

    void evict(String key);

    void clear();

    @Override
    void close();
}
