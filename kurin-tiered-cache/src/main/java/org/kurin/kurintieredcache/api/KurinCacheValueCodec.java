package org.kurin.kurintieredcache.api;

import org.kurin.kurintieredcache.core.CacheEntry;

/**
 * Turns cache entries into bytes for the tiers that store bytes (L2 and L3), metadata included.
 */
public interface KurinCacheValueCodec {

    /**
     * @throws KurinCacheException if the payload cannot be encoded
     */
    byte[] encode(CacheEntry entry);

    /**
     * @throws KurinCacheException if the bytes are not a valid entry or hold a type that may not be decoded
     */
    CacheEntry decode(byte[] bytes);

    /**
     * Reads only the metadata of an encoded entry, without touching its payload.
     *
     * @return the instant in epoch milliseconds after which the entry is useless (past its time to live and its
     * stale window), or 0 if it never expires
     */
    long deadAtMs(byte[] bytes);
}
