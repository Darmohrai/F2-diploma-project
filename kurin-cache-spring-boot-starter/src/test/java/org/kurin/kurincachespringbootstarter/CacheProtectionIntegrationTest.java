package org.kurin.kurincachespringbootstarter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.kurin.kurinlocks.api.KurinLockManager;
import org.kurin.kurinlocks.state.DistributedLockService;
import org.kurin.kurintieredcache.api.CacheTier;
import org.kurin.kurintieredcache.api.KurinCacheable;
import org.kurin.kurintieredcache.core.CacheEntry;
import org.kurin.kurintieredcache.core.KurinNullValue;
import org.kurin.kurintieredcache.provider.L2RocksDBCache;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.raft.KurinNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest(
        classes = CacheProtectionIntegrationTest.TestConfig.class,
        properties = {
                "kurin.node.host=127.0.0.1",
                "kurin.node.port=9022",
                "kurin.node.peers=",
                "kurin.node.storage=memory",
                "kurin.cache.storage-dir=target/test-cache/${random.uuid}"
        })
class CacheProtectionIntegrationTest {

    @EnableAutoConfiguration
    static class TestConfig {
        @Bean
        public CatalogService catalogService() {
            return new CatalogService();
        }

        @Bean
        public DistributedLockService distributedLockService() {
            return new DistributedLockService();
        }

        @Bean
        public KurinLockManager kurinLockManager(KurinNode kurinNode) {
            return new KurinLockManager(kurinNode, 2000, 2000);
        }
    }

    public static class CatalogService {
        private final AtomicInteger missingLookups = new AtomicInteger();
        private final AtomicInteger slowLoads = new AtomicInteger();
        private final AtomicInteger jitterLoads = new AtomicInteger();

        @KurinCacheable(cacheName = "protection-nulls", tier = CacheTier.L1_MEMORY, key = "#id", cacheNulls = true)
        public String findMissing(String id) {
            missingLookups.incrementAndGet();
            return null;
        }

        @KurinCacheable(cacheName = "protection-stampede", tier = CacheTier.L1_MEMORY, key = "#id", preventStampede = true)
        public String loadSlowly(String id) throws InterruptedException {
            slowLoads.incrementAndGet();
            Thread.sleep(300);
            return "value-" + id;
        }

        @KurinCacheable(cacheName = "protection-jitter", tier = CacheTier.L1_MEMORY, key = "#id", jitter = 20)
        public String loadWithJitter(String id) {
            jitterLoads.incrementAndGet();
            return "value-" + id;
        }

        // Counters are read through methods: fields of the AOP proxy itself are not initialized
        public int missingLookups() {
            return missingLookups.get();
        }

        public int slowLoads() {
            return slowLoads.get();
        }

        public int jitterLoads() {
            return jitterLoads.get();
        }
    }

    @Autowired
    private CatalogService catalogService;

    @Test
    void cacheNullsStoresNegativeResult() {
        assertNull(catalogService.findMissing("ghost"));
        assertNull(catalogService.findMissing("ghost"));

        assertEquals(1, catalogService.missingLookups());
    }

    @Test
    void jitterEntryIsUnwrappedOnRead() {
        assertEquals("value-1", catalogService.loadWithJitter("1"));
        assertEquals("value-1", catalogService.loadWithJitter("1"));

        assertEquals(1, catalogService.jitterLoads());
    }

    @Test
    void preventStampedeLetsOnlyOneThreadLoad() throws Exception {
        Thread.sleep(1000); // leader election

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<String>> results = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                results.add(pool.submit(() -> {
                    start.await();
                    return catalogService.loadSlowly("hot");
                }));
            }
            start.countDown();

            for (Future<String> result : results) {
                assertEquals("value-hot", result.get(10, TimeUnit.SECONDS));
            }
        } finally {
            pool.shutdownNow();
        }

        assertEquals(1, catalogService.slowLoads());
    }

    @Test
    void rocksDbTierKeepsTtlAndNullMarker(@TempDir Path storageDir) {
        L2RocksDBCache cache = new L2RocksDBCache("l2-format", storageDir.toString(), KryoSerializer.standalone());
        try {
            long expiresAt = System.currentTimeMillis() + 60_000;
            cache.put("ttl", new CacheEntry("payload", expiresAt));
            cache.put("null", new CacheEntry(KurinNullValue.INSTANCE, expiresAt));
            cache.put("plain", "payload");

            assertEquals(new CacheEntry("payload", expiresAt), cache.get("ttl"));
            CacheEntry nullEntry = assertInstanceOf(CacheEntry.class, cache.get("null"));
            assertInstanceOf(KurinNullValue.class, nullEntry.payload());
            assertEquals("payload", cache.get("plain"));
        } finally {
            cache.close();
        }
    }
}
