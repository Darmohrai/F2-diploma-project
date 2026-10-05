package org.kurin.kurintieredcache.provider;

import org.kurin.kurintieredcache.core.KurinCache;
import org.kurin.network.serializer.KryoSerializer;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class L2RocksDBCache implements KurinCache {
    private static final Logger log = LoggerFactory.getLogger(L2RocksDBCache.class);

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

            ByteArrayInputStream bis = new ByteArrayInputStream(valueBytes);
            return serializer.deserialize(bis);
        } catch (RocksDBException e) {
            log.error("[RocksDB] Error reading for key: {}", key, e);
            return null;
        }
    }

    @Override
    public void put(String key, Object value) {
        if (value == null) return;
        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            serializer.serialize(value, bos);

            db.put(keyBytes, bos.toByteArray());
        } catch (RocksDBException e) {
            log.error("[RocksDB] Error writing for key: {}", key, e);
        }
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