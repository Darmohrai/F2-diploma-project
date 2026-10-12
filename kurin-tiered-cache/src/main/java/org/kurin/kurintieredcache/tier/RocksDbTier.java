package org.kurin.kurintieredcache.tier;

import org.kurin.kurintieredcache.api.CacheTier;
import org.kurin.kurintieredcache.api.KurinCacheException;
import org.kurin.kurintieredcache.api.KurinCacheValueCodec;
import org.kurin.kurintieredcache.core.CacheEntry;
import org.kurin.kurintieredcache.core.CacheLookup;
import org.kurin.kurintieredcache.core.KurinCache;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The on-disk tier: one RocksDB database per cache, LZ4-compressed. The expiry of an entry is part of its
 * encoded header; it is enforced on every read, and a background sweep removes expired entries nobody reads.
 */
public class RocksDbTier implements KurinCache {
    private static final Logger log = LoggerFactory.getLogger(RocksDbTier.class);

    // UTF-8 never produces the byte 0xFF, so this bound is above every key
    private static final byte[] FIRST_KEY = new byte[0];
    private static final byte[] PAST_LAST_KEY = {(byte) 0xFF};

    static {
        RocksDB.loadLibrary();
    }

    private final String name;
    private final KurinCacheValueCodec codec;
    private final Options options;
    private final RocksDB db;
    // Operations share the read lock; closing takes the write lock so that no call touches a closed database
    private final ReadWriteLock lifecycle = new ReentrantReadWriteLock();
    private boolean closed;
    private byte[] sweepCursor;

    public RocksDbTier(String name, Path directory, KurinCacheValueCodec codec) {
        this.name = name;
        this.codec = codec;
        try {
            Files.createDirectories(directory);
            this.options = new Options().setCreateIfMissing(true).setCompressionType(CompressionType.LZ4_COMPRESSION);
            this.db = RocksDB.open(options, directory.toString());
        } catch (IOException | RocksDBException e) {
            throw new KurinCacheException("Cannot open the disk tier of cache '" + name + "' in " + directory
                    + ". Each application instance needs its own directory.", e);
        }
    }

    @Override
    public String name() {
        return name;
    }

    private static byte[] bytesOf(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public CacheLookup get(String key) {
        lifecycle.readLock().lock();
        try {
            if (closed) {
                return CacheLookup.miss();
            }
            byte[] keyBytes = bytesOf(key);
            byte[] value = db.get(keyBytes);
            if (value == null) {
                return CacheLookup.miss();
            }
            CacheEntry entry;
            try {
                entry = codec.decode(value);
            } catch (KurinCacheException e) {
                // Written by an older format or a class that no longer exists: useless, so it goes
                log.debug("Dropping undecodable entry '{}' of cache '{}': {}", key, name, e.getMessage());
                db.delete(keyBytes);
                return CacheLookup.miss();
            }
            if (entry.isDead(System.currentTimeMillis())) {
                db.delete(keyBytes);
                return CacheLookup.miss();
            }
            return CacheLookup.hit(entry, CacheTier.L2);
        } catch (RocksDBException e) {
            throw new KurinCacheException("Cannot read '" + key + "' from the disk tier of cache '" + name + "'", e);
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    @Override
    public void put(String key, CacheEntry entry) {
        byte[] value = codec.encode(entry);
        lifecycle.readLock().lock();
        try {
            if (!closed) {
                db.put(bytesOf(key), value);
            }
        } catch (RocksDBException e) {
            throw new KurinCacheException("Cannot write '" + key + "' to the disk tier of cache '" + name + "'", e);
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    @Override
    public void evict(String key) {
        lifecycle.readLock().lock();
        try {
            if (!closed) {
                db.delete(bytesOf(key));
            }
        } catch (RocksDBException e) {
            throw new KurinCacheException("Cannot delete '" + key + "' from the disk tier of cache '" + name + "'", e);
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    @Override
    public void clear() {
        lifecycle.readLock().lock();
        try {
            if (!closed) {
                db.deleteRange(FIRST_KEY, PAST_LAST_KEY);
            }
        } catch (RocksDBException e) {
            throw new KurinCacheException("Cannot clear the disk tier of cache '" + name + "'", e);
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    /**
     * Examines up to {@code maxEntries} entries, continuing where the previous call stopped, and deletes those
     * past their lifetime. Calling it periodically walks the whole database at a bounded rate.
     *
     * @return the number of entries removed
     */
    public int sweepExpired(int maxEntries) {
        lifecycle.readLock().lock();
        try {
            if (closed) {
                return 0;
            }
            long now = System.currentTimeMillis();
            int examined = 0;
            int removed = 0;
            try (RocksIterator iterator = db.newIterator()) {
                if (sweepCursor == null) {
                    iterator.seekToFirst();
                } else {
                    iterator.seek(sweepCursor);
                }
                while (iterator.isValid() && examined < maxEntries) {
                    examined++;
                    if (isExpired(iterator.value(), now)) {
                        db.delete(iterator.key());
                        removed++;
                    }
                    iterator.next();
                }
                // Null restarts from the beginning on the next call
                sweepCursor = iterator.isValid() ? iterator.key() : null;
            }
            return removed;
        } catch (RocksDBException e) {
            log.warn("Sweep of the disk tier of cache '{}' failed: {}", name, e.getMessage());
            return 0;
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    private boolean isExpired(byte[] value, long now) {
        try {
            long deadAtMs = codec.deadAtMs(value);
            return deadAtMs != 0 && now >= deadAtMs;
        } catch (KurinCacheException e) {
            return true; // not an entry we can ever serve
        }
    }

    @Override
    public void close() {
        lifecycle.writeLock().lock();
        try {
            if (!closed) {
                closed = true;
                db.close();
                options.close();
            }
        } finally {
            lifecycle.writeLock().unlock();
        }
    }
}
