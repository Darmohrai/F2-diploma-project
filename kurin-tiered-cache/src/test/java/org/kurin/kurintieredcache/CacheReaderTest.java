package org.kurin.kurintieredcache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.kurin.kurintieredcache.api.CacheResult;
import org.kurin.kurintieredcache.api.CacheTier;
import org.kurin.kurintieredcache.api.KurinCacheObserver;
import org.kurin.kurintieredcache.api.KurinCacheTrace;
import org.kurin.kurintieredcache.api.OnLockTimeout;
import org.kurin.kurintieredcache.api.StampedeMode;
import org.kurin.kurintieredcache.core.CacheEntry;
import org.kurin.kurintieredcache.core.CacheReader;
import org.kurin.kurintieredcache.core.CacheReader.ReadOptions;
import org.kurin.kurintieredcache.core.CacheSettings;
import org.kurin.kurintieredcache.core.KurinCacheManager;
import org.kurin.kurintieredcache.core.TieredCache;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class CacheReaderTest {

    @TempDir
    Path dataDirectory;

    private KurinCacheManager manager;
    private CacheReader reader;
    private ExecutorService callers;
    private final List<String> events = new CopyOnWriteArrayList<>();

    @BeforeEach
    void start() {
        manager = KurinCacheManager.builder()
                .dataDirectory(dataDirectory)
                .observer(new KurinCacheObserver() {
                    @Override
                    public void onLookup(String cache, String key, CacheResult result) {
                        events.add(result.name());
                    }

                    @Override
                    public void onStampedeWait(String cache, String key, StampedeMode mode, Duration waited) {
                        events.add("WAIT_" + mode);
                    }
                })
                .build();
        reader = new CacheReader(manager, null);
    }

    @AfterEach
    void stop() {
        if (callers != null) {
            callers.shutdownNow();
        }
        reader.close();
        manager.close();
    }

    private static ReadOptions options(CacheSettings settings, StampedeMode stampede, boolean cacheNulls) {
        return new ReadOptions(settings, cacheNulls, stampede, Duration.ofSeconds(1), OnLockTimeout.LOAD, null);
    }

    private static CacheSettings ttl(Duration ttl) {
        return CacheSettings.defaults().withTtl(ttl);
    }

    @Test
    void secondReadIsAHitAndLowerTierHitsAreCopiedUp() throws Exception {
        TieredCache cache = manager.getCache("products");
        AtomicInteger loads = new AtomicInteger();
        ReadOptions options = options(ttl(Duration.ofMinutes(5)), StampedeMode.LOCAL, false);

        assertThat(reader.read(cache, "p1", options, () -> "value-" + loads.incrementAndGet())).isEqualTo("value-1");
        assertThat(reader.read(cache, "p1", options, () -> "value-" + loads.incrementAndGet())).isEqualTo("value-1");
        assertThat(events).containsExactly("MISS", "HIT_L1");

        // A new manager on the same directory has an empty memory tier: the disk tier answers, once
        manager.close();
        reader.close();
        events.clear();
        start();
        TieredCache reopened = manager.getCache("products");
        assertThat(reader.read(reopened, "p1", options, () -> "value-" + loads.incrementAndGet())).isEqualTo("value-1");
        assertThat(reader.read(reopened, "p1", options, () -> "value-" + loads.incrementAndGet())).isEqualTo("value-1");
        assertThat(events).containsExactly("HIT_L2", "HIT_L1");
        assertThat(loads.get()).isEqualTo(1);
    }

    @Test
    void nullsAreCachedOnlyOnRequestAndWithTheirOwnTtl() throws Exception {
        TieredCache cache = manager.getCache("lookups");
        AtomicInteger loads = new AtomicInteger();

        ReadOptions notCachingNulls = options(ttl(Duration.ofMinutes(5)), StampedeMode.NONE, false);
        assertThat(reader.read(cache, "ghost", notCachingNulls, () -> { loads.incrementAndGet(); return null; })).isNull();
        assertThat(reader.read(cache, "ghost", notCachingNulls, () -> { loads.incrementAndGet(); return null; })).isNull();
        assertThat(loads.get()).isEqualTo(2);

        ReadOptions cachingNulls = options(ttl(Duration.ofMinutes(5)).withNullTtl(Duration.ofMillis(150)), StampedeMode.NONE, true);
        assertThat(reader.read(cache, "ghost", cachingNulls, () -> { loads.incrementAndGet(); return null; })).isNull();
        assertThat(reader.read(cache, "ghost", cachingNulls, () -> { loads.incrementAndGet(); return null; })).isNull();
        assertThat(loads.get()).isEqualTo(3);

        CacheEntry entry = cache.get("ghost").entry();
        assertThat(entry.isNullMarker()).isTrue();
        assertThat(entry.expiresAtMs() - entry.createdAtMs()).isEqualTo(150);

        // The negative entry outlives neither tier: after its short ttl the source is asked again
        await().atMost(Duration.ofSeconds(5)).until(() -> System.currentTimeMillis() >= entry.expiresAtMs());
        assertThat(cache.get("ghost").isHit()).isFalse();
        assertThat(reader.read(cache, "ghost", cachingNulls, () -> { loads.incrementAndGet(); return "now it exists"; })).isEqualTo("now it exists");
        assertThat(loads.get()).isEqualTo(4);
    }

    // Spec test 19, memory and disk tiers
    @Test
    void ttlIsEnforcedInMemoryAndOnDisk() throws Exception {
        TieredCache both = manager.getCache("both");
        TieredCache diskOnly = manager.getCache("disk-only", List.of(CacheTier.L2));
        TieredCache memoryOnly = manager.getCache("memory-only", List.of(CacheTier.L1));
        assertThat(diskOnly.tiers()).containsExactly(CacheTier.L2);
        assertThat(memoryOnly.tiers()).containsExactly(CacheTier.L1);

        CacheSettings shortLived = ttl(Duration.ofMillis(200));
        long now = System.currentTimeMillis();
        for (TieredCache cache : List.of(both, diskOnly, memoryOnly)) {
            cache.put("k", shortLived.newEntry("v", now));
            cache.put("forever", ttl(Duration.ZERO).newEntry("v", now));
            assertThat(cache.get("k").isHit()).isTrue();
        }

        await().atMost(Duration.ofSeconds(5)).until(() -> System.currentTimeMillis() >= now + 200);
        for (TieredCache cache : List.of(both, diskOnly, memoryOnly)) {
            assertThat(cache.get("k").isHit()).as("expired entry in %s", cache.name()).isFalse();
            assertThat(cache.get("forever").isHit()).isTrue();
        }
    }

    // Spec test 19, clear() of the disk tier
    @Test
    void clearRemovesEverythingFromTheDiskTierAndExpiredEntriesAreSwept() {
        TieredCache cache = manager.getCache("catalog", List.of(CacheTier.L2));
        long now = System.currentTimeMillis();
        for (int i = 0; i < 500; i++) {
            cache.put("key-" + i, ttl(Duration.ofMinutes(5)).newEntry("value-" + i, now));
        }
        assertThat(cache.get("key-499").isHit()).isTrue();

        cache.clear();
        for (int i = 0; i < 500; i++) {
            assertThat(cache.get("key-" + i).isHit()).isFalse();
        }

        // Entries nobody reads are removed by the sweep, a bounded batch at a time
        for (int i = 0; i < 30; i++) {
            cache.put("dead-" + i, ttl(Duration.ofMillis(1)).newEntry("x", now - 1000));
        }
        cache.put("alive", ttl(Duration.ofMinutes(5)).newEntry("x", now));
        int removed = 0;
        for (int round = 0; round < 10 && removed < 30; round++) {
            removed += cache.sweepDisk(10);
        }
        assertThat(removed).isEqualTo(30);
        assertThat(cache.get("alive").isHit()).isTrue();
    }

    @Test
    void jitterSpreadsExpiryAroundTheTtl() {
        CacheSettings settings = ttl(Duration.ofSeconds(100)).withJitterPercent(20);
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (int i = 0; i < 2000; i++) {
            long lifetime = settings.newEntry("v", 0).expiresAtMs();
            min = Math.min(min, lifetime);
            max = Math.max(max, lifetime);
        }
        assertThat(min).isBetween(80_000L, 82_000L);
        assertThat(max).isBetween(118_000L, 120_000L);
        assertThat(ttl(Duration.ofSeconds(100)).newEntry("v", 0).expiresAtMs()).isEqualTo(100_000);
    }

    // Spec test 21, per-instance part
    @Test
    void localSingleFlightLoadsOnceForTwoHundredConcurrentCallers() throws Exception {
        TieredCache cache = manager.getCache("hot");
        ReadOptions options = options(ttl(Duration.ofMinutes(5)), StampedeMode.LOCAL, false);
        int threads = 200;
        callers = Executors.newFixedThreadPool(threads);
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);

        List<Future<Object>> results = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            results.add(callers.submit(() -> {
                start.await();
                return reader.read(cache, "item", options, () -> {
                    loads.incrementAndGet();
                    loading.countDown();
                    // Holds the load open until the other callers had every chance to pile up behind it
                    finish.await(10, TimeUnit.SECONDS);
                    return "loaded";
                });
            }));
        }
        start.countDown();
        assertThat(loading.await(10, TimeUnit.SECONDS)).isTrue();
        await().atMost(Duration.ofSeconds(10)).until(() -> events.stream().filter("MISS"::equals).count() == threads);
        finish.countDown();

        for (Future<Object> result : results) {
            assertThat(result.get(10, TimeUnit.SECONDS)).isEqualTo("loaded");
        }
        assertThat(loads.get()).isEqualTo(1);
        assertThat(events.stream().filter("WAIT_LOCAL"::equals).count()).isEqualTo(threads - 1);
    }

    @Test
    void failedLoadReachesEveryWaiterAndIsNotCached() {
        TieredCache cache = manager.getCache("failing");
        ReadOptions options = options(ttl(Duration.ofMinutes(5)), StampedeMode.LOCAL, false);

        assertThatThrownBy(() -> reader.read(cache, "k", options, () -> {
            throw new IllegalStateException("source down");
        })).isInstanceOf(IllegalStateException.class).hasMessage("source down");
        assertThat(cache.get("k").isHit()).isFalse();
    }

    // Spec test 22
    @Test
    void staleValueIsServedAtOnceWhileOneRefreshRuns() throws Exception {
        TieredCache cache = manager.getCache("prices");
        CacheSettings settings = ttl(Duration.ofMillis(150)).withStaleWhileRevalidate(Duration.ofSeconds(30));
        ReadOptions options = options(settings, StampedeMode.LOCAL, false);
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch refreshMayFinish = new CountDownLatch(1);

        assertThat(reader.read(cache, "sku", options, () -> "v" + loads.incrementAndGet())).isEqualTo("v1");
        long expiresAt = cache.get("sku").entry().expiresAtMs();
        await().atMost(Duration.ofSeconds(5)).until(() -> System.currentTimeMillis() >= expiresAt);
        events.clear();

        // Expired, inside the stale window: every caller gets the old value immediately, one refresh starts
        for (int i = 0; i < 20; i++) {
            Object value = reader.read(cache, "sku", options, () -> {
                int load = loads.incrementAndGet();
                refreshMayFinish.await(10, TimeUnit.SECONDS);
                return "v" + load;
            });
            assertThat(value).isEqualTo("v1");
        }
        assertThat(events).containsOnly("STALE");
        await().atMost(Duration.ofSeconds(5)).until(() -> loads.get() == 2);
        // Twenty readers, one refresh: it is still running, so nothing else can have loaded
        assertThat(loads.get()).isEqualTo(2);

        refreshMayFinish.countDown();
        // The refreshed value lives 150 ms too. A read that comes later than that gets it as a stale value and
        // starts the next refresh, so the number of loads from here on depends on how fast this thread is and
        // is not asserted.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(reader.read(cache, "sku", options, () -> "v" + loads.incrementAndGet())).isEqualTo("v2"));
    }

    @Test
    void failedRefreshKeepsTheStaleValue() throws Exception {
        TieredCache cache = manager.getCache("fragile");
        ReadOptions options = options(ttl(Duration.ofMillis(100)).withStaleWhileRevalidate(Duration.ofSeconds(30)), StampedeMode.LOCAL, false);
        AtomicInteger loads = new AtomicInteger();

        reader.read(cache, "k", options, () -> "good");
        long expiresAt = cache.get("k").entry().expiresAtMs();
        await().atMost(Duration.ofSeconds(5)).until(() -> System.currentTimeMillis() >= expiresAt);

        assertThat(reader.read(cache, "k", options, () -> {
            loads.incrementAndGet();
            throw new IllegalStateException("source down");
        })).isEqualTo("good");
        await().atMost(Duration.ofSeconds(5)).until(() -> loads.get() >= 1);
        assertThat(cache.get("k").entry().value()).isEqualTo("good");
    }

    // Spec test 18
    @Test
    void loadThatStartedBeforeAnInvalidationDoesNotWriteItsStaleResult() throws Exception {
        TieredCache cache = manager.getCache("accounts");
        ReadOptions options = options(ttl(Duration.ofMinutes(5)), StampedeMode.LOCAL, false);
        callers = Executors.newSingleThreadExecutor();
        CountDownLatch sourceRead = new CountDownLatch(1);
        CountDownLatch invalidated = new CountDownLatch(1);
        String[] source = {"old"};

        Future<Object> slowReader = callers.submit(() -> reader.read(cache, "acc-1", options, () -> {
            String seen = source[0];
            sourceRead.countDown();
            // The write and its invalidation happen while this reader is still on its way back from the source
            invalidated.await(10, TimeUnit.SECONDS);
            return seen;
        }));

        assertThat(sourceRead.await(10, TimeUnit.SECONDS)).isTrue();
        source[0] = "new";
        cache.evictLocal("acc-1", "test");
        invalidated.countDown();

        // The slow reader still returns what it read, but that value must not stay in the cache
        assertThat(slowReader.get(10, TimeUnit.SECONDS)).isEqualTo("old");
        assertThat(cache.get("acc-1").isHit()).isFalse();
        assertThat(reader.read(cache, "acc-1", options, () -> source[0])).isEqualTo("new");
    }

    @Test
    void loadThatStartedBeforeAClearDoesNotWriteItsStaleResult() throws Exception {
        TieredCache cache = manager.getCache("ledger");
        ReadOptions options = options(ttl(Duration.ofMinutes(5)), StampedeMode.NONE, false);
        callers = Executors.newSingleThreadExecutor();
        CountDownLatch sourceRead = new CountDownLatch(1);
        CountDownLatch cleared = new CountDownLatch(1);

        Future<Object> slowReader = callers.submit(() -> reader.read(cache, "row", options, () -> {
            sourceRead.countDown();
            cleared.await(10, TimeUnit.SECONDS);
            return "old";
        }));
        assertThat(sourceRead.await(10, TimeUnit.SECONDS)).isTrue();
        cache.clearLocal("test");
        cleared.countDown();

        assertThat(slowReader.get(10, TimeUnit.SECONDS)).isEqualTo("old");
        assertThat(cache.get("row").isHit()).isFalse();
    }

    @Test
    void unlessKeepsAValueOutOfTheCache() throws Exception {
        TieredCache cache = manager.getCache("filtered");
        ReadOptions options = new ReadOptions(ttl(Duration.ofMinutes(5)), false, StampedeMode.NONE, Duration.ofSeconds(1),
                OnLockTimeout.LOAD, value -> "skip".equals(value));

        assertThat(reader.read(cache, "a", options, () -> "skip")).isEqualTo("skip");
        assertThat(cache.get("a").isHit()).isFalse();
        assertThat(reader.read(cache, "b", options, () -> "keep")).isEqualTo("keep");
        assertThat(cache.get("b").isHit()).isTrue();
    }

    @Test
    void traceRecordsTheLookupsOfTheCurrentThread() throws Exception {
        TieredCache cache = manager.getCache("traced");
        ReadOptions options = options(ttl(Duration.ofMinutes(5)), StampedeMode.NONE, false);

        reader.read(cache, "before", options, () -> "x");
        KurinCacheTrace.begin();
        reader.read(cache, "k", options, () -> "v");
        reader.read(cache, "k", options, () -> "v");
        List<KurinCacheTrace.Lookup> lookups = KurinCacheTrace.end();

        assertThat(lookups).containsExactly(
                new KurinCacheTrace.Lookup("traced", "k", CacheResult.MISS),
                new KurinCacheTrace.Lookup("traced", "k", CacheResult.HIT_L1));
        assertThat(KurinCacheTrace.lookups()).isEmpty();
    }
}
