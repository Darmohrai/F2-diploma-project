package org.kurin.kurincachespringbootstarter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kurin.kurintieredcache.api.CacheTier;
import org.kurin.kurintieredcache.api.KurinCacheException;
import org.kurin.kurintieredcache.core.CacheReader;
import org.kurin.kurintieredcache.core.CacheReader.ReadOptions;
import org.kurin.kurintieredcache.core.CacheSettings;
import org.kurin.kurintieredcache.core.KurinCacheManager;
import org.kurin.kurintieredcache.core.TieredCache;
import org.kurin.kurintieredcache.api.OnLockTimeout;
import org.kurin.kurintieredcache.api.StampedeMode;
import org.kurin.kurintieredcache.tier.CircuitBreaker;
import org.kurin.test.InMemorySharedExternalStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * The external tier against a store that expires, hangs and fails.
 */
class ExternalTierTest {
    private static final List<String> ALLOWED = List.of("org.kurin");

    private final InMemorySharedExternalStore store = new InMemorySharedExternalStore();
    private KurinCacheManager manager;
    private CacheReader reader;

    @AfterEach
    void stop() {
        store.resume();
        if (reader != null) {
            reader.close();
        }
        if (manager != null) {
            manager.close();
        }
    }

    private KurinCacheManager manager(Duration timeout, int failureThreshold, Duration openDuration) {
        manager = KurinCacheManager.builder()
                .externalStore(store)
                .allowedPackages(ALLOWED)
                .externalTimeout(timeout)
                .externalCircuit(failureThreshold, openDuration)
                .build();
        reader = new CacheReader(manager, null);
        return manager;
    }

    private static CacheSettings ttl(Duration ttl) {
        return CacheSettings.defaults().withTtl(ttl);
    }

    private static ReadOptions options(CacheSettings settings, boolean cacheNulls) {
        return new ReadOptions(settings, cacheNulls, StampedeMode.NONE, Duration.ofSeconds(1), OnLockTimeout.LOAD, null);
    }

    @Test
    void sharedStoreWithoutAllowedPackagesIsRefused() {
        assertThatThrownBy(() -> KurinCacheManager.builder().externalStore(store).build())
                .isInstanceOf(KurinCacheException.class)
                .hasMessageContaining("allowed-packages");
    }

    // Spec test 19, external tier
    @Test
    void ttlAndNullTtlReachTheExternalStore() throws Exception {
        TieredCache cache = manager(Duration.ofSeconds(2), 5, Duration.ofSeconds(10)).getCache("offers", List.of(CacheTier.L3));
        assertThat(cache.tiers()).containsExactly(CacheTier.L3);
        ReadOptions options = options(ttl(Duration.ofMinutes(5)).withNullTtl(Duration.ofMillis(300)), true);
        AtomicInteger loads = new AtomicInteger();

        assertThat(reader.read(cache, "present", options, () -> "value-" + loads.incrementAndGet())).isEqualTo("value-1");
        assertThat(reader.read(cache, "absent", options, () -> {
            loads.incrementAndGet();
            return null;
        })).isNull();
        await().atMost(Duration.ofSeconds(5)).until(() -> store.contains("kurin:offers:present") && store.contains("kurin:offers:absent"));

        // Both are served from the store now, the null included
        assertThat(reader.read(cache, "present", options, () -> "value-" + loads.incrementAndGet())).isEqualTo("value-1");
        assertThat(reader.read(cache, "absent", options, () -> "should not load")).isNull();
        assertThat(loads.get()).isEqualTo(2);

        // The store itself drops the negative entry after its short ttl; the positive one stays
        await().atMost(Duration.ofSeconds(5)).until(() -> !store.contains("kurin:offers:absent"));
        assertThat(store.contains("kurin:offers:present")).isTrue();
        assertThat(reader.read(cache, "absent", options, () -> "appeared")).isEqualTo("appeared");
    }

    @Test
    void clearRemovesOnlyTheKeysOfItsOwnCache() {
        KurinCacheManager caches = manager(Duration.ofSeconds(2), 5, Duration.ofSeconds(10));
        TieredCache first = caches.getCache("first", List.of(CacheTier.L3));
        TieredCache second = caches.getCache("second", List.of(CacheTier.L3));
        long now = System.currentTimeMillis();
        for (int i = 0; i < 20; i++) {
            first.put("k" + i, ttl(Duration.ofMinutes(5)).newEntry("v", now));
            second.put("k" + i, ttl(Duration.ofMinutes(5)).newEntry("v", now));
        }
        await().atMost(Duration.ofSeconds(5)).until(() -> store.size() == 40);

        first.clear();

        await().atMost(Duration.ofSeconds(5)).until(() -> store.size() == 20);
        assertThat(first.get("k3").isHit()).isFalse();
        assertThat(second.get("k3").isHit()).isTrue();
    }

    // Spec test 23
    @Test
    void hangingStoreNeverDelaysReadsBeyondTheTimeoutAndOpensTheCircuit() throws Exception {
        Duration timeout = Duration.ofMillis(100);
        Duration openDuration = Duration.ofMillis(600);
        TieredCache cache = manager(timeout, 3, openDuration).getCache("slow", List.of(CacheTier.L3));
        ReadOptions options = options(ttl(Duration.ofMinutes(5)), false);

        assertThat(reader.read(cache, "warm", options, () -> "cached")).isEqualTo("cached");
        await().atMost(Duration.ofSeconds(5)).until(() -> store.contains("kurin:slow:warm"));

        store.stall(Duration.ofSeconds(5));
        long slack = TimeUnit.MILLISECONDS.toNanos(150);

        // Each read waits for the store at most the timeout, then goes to the source
        for (int i = 0; i < 3; i++) {
            long started = System.nanoTime();
            assertThat(reader.read(cache, "warm", options, () -> "from source")).isEqualTo("from source");
            assertThat(System.nanoTime() - started).isLessThan(timeout.toNanos() * 2 + slack);
        }
        await().atMost(Duration.ofSeconds(5)).until(() -> cache.externalCircuitState() == CircuitBreaker.State.OPEN);

        // With the circuit open the store is not called at all: reads are immediate
        // Writes queued before the circuit opened may still be on their way into the store: wait until it is quiet
        int[] lastSeen = {-1};
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(150)).until(() -> {
            int seen = store.calls();
            boolean quiet = seen == lastSeen[0];
            lastSeen[0] = seen;
            return quiet;
        });
        int callsWhenOpened = store.calls();
        long started = System.nanoTime();
        for (int i = 0; i < 50; i++) {
            assertThat(reader.read(cache, "warm", options, () -> "from source")).isEqualTo("from source");
        }
        assertThat(System.nanoTime() - started).isLessThan(timeout.toNanos());
        assertThat(store.calls()).isEqualTo(callsWhenOpened);

        // Half-open: exactly one probe reaches the store, however many readers arrive
        await().atMost(Duration.ofSeconds(5)).until(() -> cache.externalCircuitState() == CircuitBreaker.State.HALF_OPEN);
        List<Thread> readers = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Thread thread = new Thread(() -> {
                try {
                    reader.read(cache, "warm", options, () -> "from source");
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            readers.add(thread);
            thread.start();
        }
        for (Thread thread : readers) {
            thread.join(5000);
        }
        // The probe is one read; the writes the readers queue afterwards are refused by the reopened circuit
        assertThat(store.operations().subList(callsWhenOpened, store.operations().size())).containsExactly("get:kurin:slow:warm");
        assertThat(cache.externalCircuitState()).isEqualTo(CircuitBreaker.State.OPEN);

        // The store recovers: the next probe closes the circuit and the cache is used again
        store.resume();
        await().atMost(Duration.ofSeconds(5)).until(() -> cache.externalCircuitState() == CircuitBreaker.State.HALF_OPEN);
        // (calls that were hanging finished meanwhile, so the stored value may be the one written during the outage)
        assertThat(reader.read(cache, "warm", options, () -> "reloaded")).isNotEqualTo("reloaded");
        assertThat(cache.externalCircuitState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void failingStoreIsAMissNotAnError() throws Exception {
        TieredCache cache = manager(Duration.ofMillis(200), 2, Duration.ofSeconds(30)).getCache("broken", List.of(CacheTier.L3));
        ReadOptions options = options(ttl(Duration.ofMinutes(5)), false);
        store.fail(new IllegalStateException("connection refused"));

        for (int i = 0; i < 10; i++) {
            assertThat(reader.read(cache, "k", options, () -> "from source")).isEqualTo("from source");
        }
        assertThat(cache.externalCircuitState()).isEqualTo(CircuitBreaker.State.OPEN);
    }

    // Spec test 24
    @Test
    void writesAndDeletesOfOneKeyReachTheStoreInOrder() {
        TieredCache cache = manager(Duration.ofSeconds(2), 5, Duration.ofSeconds(10)).getCache("ordered", List.of(CacheTier.L3));
        CacheSettings settings = ttl(Duration.ofMinutes(5));
        int rounds = 200;
        List<String> expected = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (int round = 0; round < rounds; round++) {
            for (String key : List.of("a", "b", "c", "d")) {
                cache.put(key, settings.newEntry("value-" + round, now));
                expected.add("put:kurin:ordered:" + key);
                if (round % 3 == 0) {
                    cache.evict(key);
                    expected.add("delete:kurin:ordered:" + key);
                }
            }
        }
        // The last round (199) ends with a put for every key
        await().atMost(Duration.ofSeconds(10)).until(() -> store.operations().size() == expected.size());

        for (String key : List.of("a", "b", "c", "d")) {
            String suffix = ":kurin:ordered:" + key;
            assertThat(store.operations().stream().filter(operation -> operation.endsWith(suffix)).toList())
                    .containsExactlyElementsOf(expected.stream().filter(operation -> operation.endsWith(suffix)).toList());
            assertThat(cache.get(key).entry().value()).isEqualTo("value-" + (rounds - 1));
        }
    }

    @Test
    void foreignBytesInTheSharedStoreAreIgnored() throws Exception {
        TieredCache cache = manager(Duration.ofSeconds(2), 5, Duration.ofSeconds(10)).getCache("guarded", List.of(CacheTier.L3));
        ReadOptions options = options(ttl(Duration.ofMinutes(5)), false);
        store.putRaw("kurin:guarded:k", new byte[]{'n', 'o', 't', ' ', 'o', 'u', 'r', 's'});

        assertThat(reader.read(cache, "k", options, () -> "from source")).isEqualTo("from source");
        assertThat(cache.externalCircuitState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }
}
