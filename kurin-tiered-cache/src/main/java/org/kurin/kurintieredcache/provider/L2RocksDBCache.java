package org.kurin.kurintieredcache.provider;

import org.kurin.kurintieredcache.core.CacheEntry;
import org.kurin.kurintieredcache.core.KurinCache;
import org.kurin.kurintieredcache.core.KurinNullValue;
import org.kurin.network.serializer.KryoSerializer;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class L2RocksDBCache implements KurinCache {
    private static final Logger log = LoggerFactory.getLogger(L2RocksDBCache.class);

    // Value layout: MAGIC | flags | [expiresAtMs] | [Kryo payload].
    // CacheEntry and KurinNullValue are framework types that are not registered in Kryo,
    // so they are encoded in the header instead of being serialized.
    private static final byte[] MAGIC = {0x4B, 0x43};
    private static final int FLAG_EXPIRES = 1;
    private static final int FLAG_NULL = 2;

    private final String name;
    private final RocksDB db;
    private final KryoSerializer serializer;

    static {
        RocksDB.loadLibrary();
    }

    public L2RocksDBCache(String name, String storageDir, KryoSerializer serializer) {
        this.name = name;
        this.serializer = serializer;

        Path dbPath = Paths.get(storageDir, name);
        try {
            Files.createDirectories(dbPath);
            try (Options options = new Options().setCreateIfMissing(true)) {
                this.db = RocksDB.open(options, dbPath.toString());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize RocksDB for L2 cache: " + name, e);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object get(String key) {
        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = db.get(keyBytes);
            if (valueBytes == null) {
                return null;
            }

            if (!hasHeader(valueBytes)) {
                // Written before the header was introduced
                return serializer.deserialize(new ByteArrayInputStream(valueBytes));
            }

            ByteBuffer buffer = ByteBuffer.wrap(valueBytes, MAGIC.length, valueBytes.length - MAGIC.length);
            int flags = buffer.get();
            long expiresAtMs = (flags & FLAG_EXPIRES) != 0 ? buffer.getLong() : 0;

            Object payload = (flags & FLAG_NULL) != 0
                    ? KurinNullValue.INSTANCE
                    : serializer.deserialize(new ByteArrayInputStream(valueBytes, buffer.position(), buffer.remaining()));

            return expiresAtMs > 0 ? new CacheEntry(payload, expiresAtMs) : payload;
        } catch (RocksDBException e) {
            log.error("[RocksDB] Error reading for key: {}", key, e);
            return null;
        }
    }

    @Override
    public void put(String key, Object value) {
        Object payload = value;
        long expiresAtMs = 0;
        if (value instanceof CacheEntry entry) {
            payload = entry.payload();
            expiresAtMs = entry.expiresAtMs();
        }
        if (payload == null) return;

        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            boolean isNull = payload instanceof KurinNullValue;

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.writeBytes(MAGIC);
            bos.write((expiresAtMs > 0 ? FLAG_EXPIRES : 0) | (isNull ? FLAG_NULL : 0));
            if (expiresAtMs > 0) {
                bos.writeBytes(ByteBuffer.allocate(Long.BYTES).putLong(expiresAtMs).array());
            }
            if (!isNull) {
                serializer.serialize(payload, bos);
            }

            db.put(keyBytes, bos.toByteArray());
        } catch (RocksDBException e) {
            log.error("[RocksDB] Error writing for key: {}", key, e);
        }
    }

    private boolean hasHeader(byte[] valueBytes) {
        return valueBytes.length > MAGIC.length && valueBytes[0] == MAGIC[0] && valueBytes[1] == MAGIC[1];
    }

    @Override
    public void evict(String key) {
        try {
            db.delete(key.getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            log.error("[RocksDB] Error deleting key: {}", key, e);
        }
    }

    @Override
    public void clear() {
        log.warn("[RocksDB] clear() called for cache {}. It is recommended to use evict() for specific keys.", name);
    }

    @Override
    public void close() {
        if (db != null) {
            log.info("[RocksDB] Closing native resources for cache: {}", name);
            db.close();
        }
    }
}
