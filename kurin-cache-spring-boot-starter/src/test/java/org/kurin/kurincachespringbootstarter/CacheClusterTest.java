package org.kurin.kurincachespringbootstarter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.kurin.api.KurinCommand;
import org.kurin.kurinspringbootstarter.template.KurinTemplate;
import org.kurin.kurintieredcache.api.ExternalCacheStore;
import org.kurin.kurintieredcache.api.KurinCacheEvict;
import org.kurin.kurintieredcache.api.KurinCacheInvalidator;
import org.kurin.kurintieredcache.api.KurinCachePut;
import org.kurin.kurintieredcache.api.KurinCacheable;
import org.kurin.kurintieredcache.api.StampedeMode;
import org.kurin.kurintieredcache.core.KurinCacheManager;
import org.kurin.kurintieredcache.core.TieredCache;
import org.kurin.raft.KurinNode;
import org.kurin.test.InMemorySharedExternalStore;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Three instances of one application sharing a database and an external cache store.
 */
class CacheClusterTest {

    static final class Shared {
        static volatile InMemorySharedExternalStore store = new InMemorySharedExternalStore();
        static final AtomicInteger CATALOG_LOADS = new AtomicInteger();
    }

    @EnableAutoConfiguration
    static class App {
        @Bean
        ExternalCacheStore externalCacheStore() {
            return Shared.store;
        }

        @Bean
        Accounts accounts(JdbcTemplate jdbc) {
            return new Accounts(jdbc);
        }

        @Bean
        Catalog catalog() {
            return new Catalog();
        }

        @Bean
        Prices prices() {
            return new Prices();
        }
    }

    /**
     * The usual shape of an application service: the database is the source of truth, written once by
     * whichever instance handles the request.
     */
    public static class Accounts {
        private final JdbcTemplate jdbc;
        private final AtomicInteger loads = new AtomicInteger();

        public Accounts(JdbcTemplate jdbc) {
            this.jdbc = jdbc;
        }

        @KurinCacheable(cacheName = "accounts", key = "#id")
        public String nameOf(String id) {
            loads.incrementAndGet();
            return jdbc.queryForObject("select name from accounts where id = ?", String.class, id);
        }

        @Transactional
        @KurinCacheEvict(cacheNames = "accounts", key = "#id")
        public void rename(String id, String name) {
            jdbc.update("update accounts set name = ? where id = ?", name, id);
        }

        @Transactional
        @KurinCacheEvict(cacheNames = "accounts", key = "#id")
        public void renameAndFail(String id, String name) {
            jdbc.update("update accounts set name = ? where id = ?", name, id);
            throw new IllegalStateException("validation failed after the update");
        }

        public int loads() {
            return loads.get();
        }
    }

    public static class Catalog {
        @KurinCacheable(cacheName = "catalog", key = "#id", stampede = StampedeMode.CLUSTER, lockWait = "PT30S")
        public String describe(String id) throws InterruptedException {
            Shared.CATALOG_LOADS.incrementAndGet();
            // An expensive query: long enough for every other caller to arrive while it runs
            new CountDownLatch(1).await(300, TimeUnit.MILLISECONDS);
            return "description of " + id;
        }
    }

    public record SetPrice(String sku, BigDecimal price) {
    }

    public record RemovePrice(String sku) {
    }

    /**
     * A replicated state machine whose commands keep a cache in step on every instance. It holds nothing
     * itself: the cache annotations act when a command is applied.
     */
    @org.kurin.api.KurinService(stateless = true)
    public static class Prices {
        @KurinCommand
        @KurinCachePut(cacheName = "prices", key = "#cmd.sku")
        public BigDecimal setPrice(SetPrice command) {
            return command.price();
        }

        @KurinCommand
        @KurinCacheEvict(cacheNames = "prices", key = "#cmd.sku")
        public void removePrice(RemovePrice command) {
        }
    }

    @TempDir
    Path directory;

    private SpringTestCluster cluster;
    private ExecutorService callers;

    @BeforeEach
    void start() {
        Shared.store = new InMemorySharedExternalStore();
        Shared.CATALOG_LOADS.set(0);
        cluster = new SpringTestCluster(3, App.class, directory,
                "spring.datasource.url=jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1",
                "kurin.cache.codec.allowed-packages=org.kurin",
                "kurin.cache.l3.timeout=2s",
                // Local tiers only, so that the state of each instance can be observed on its own
                "kurin.cache.caches.accounts.tiers=L1,L2",
                "kurin.cache.caches.prices.tiers=L1,L2");
        JdbcTemplate jdbc = cluster.contexts().get(0).getBean(JdbcTemplate.class);
        jdbc.execute("create table accounts (id varchar(32) primary key, name varchar(64))");
        jdbc.update("insert into accounts values ('a1', 'Alice')");
    }

    @AfterEach
    void stop() {
        if (callers != null) {
            callers.shutdownNow();
        }
        cluster.close();
    }

    private List<TieredCache> caches(String name) {
        return cluster.beans(KurinCacheManager.class).stream().map(manager -> manager.getCache(name)).toList();
    }

    // Spec test 17
    @Test
    void evictOnTransactionalMethodInvalidatesAllInstancesAfterCommitAndNotAfterRollback() {
        List<Accounts> instances = cluster.beans(Accounts.class);
        for (Accounts accounts : instances) {
            assertThat(accounts.nameOf("a1")).isEqualTo("Alice");
            assertThat(accounts.nameOf("a1")).isEqualTo("Alice");
            assertThat(accounts.loads()).isEqualTo(1);
        }
        assertThat(caches("accounts")).allSatisfy(cache -> assertThat(cache.get("a1").isHit()).isTrue());

        // One instance writes to the database, once
        instances.get(0).rename("a1", "Alicia");

        // Memory and disk tier of every instance lose the entry
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(caches("accounts")).allSatisfy(cache -> assertThat(cache.get("a1").isHit()).isFalse()));
        for (Accounts accounts : instances) {
            assertThat(accounts.nameOf("a1")).isEqualTo("Alicia");
            assertThat(accounts.loads()).isEqualTo(2);
        }

        // A transaction that rolls back changes nothing and invalidates nothing
        List<Long> logIndexes = cluster.beans(KurinNode.class).stream().map(node -> node.status().lastLogIndex()).toList();
        assertThatThrownBy(() -> instances.get(1).renameAndFail("a1", "Mallory")).isInstanceOf(IllegalStateException.class);

        assertThat(cluster.contexts().get(2).getBean(JdbcTemplate.class)
                .queryForObject("select name from accounts where id = 'a1'", String.class)).isEqualTo("Alicia");
        assertThat(caches("accounts")).allSatisfy(cache -> assertThat(cache.get("a1").entry().value()).isEqualTo("Alicia"));
        assertThat(cluster.beans(KurinNode.class).stream().map(node -> node.status().lastLogIndex()).toList()).isEqualTo(logIndexes);
        for (Accounts accounts : instances) {
            assertThat(accounts.nameOf("a1")).isEqualTo("Alicia");
            assertThat(accounts.loads()).isEqualTo(2);
        }
    }

    @Test
    void programmaticInvalidationCompletesWhenAppliedLocallyAndReachesAllInstances() throws Exception {
        List<Accounts> instances = cluster.beans(Accounts.class);
        instances.forEach(accounts -> accounts.nameOf("a1"));

        KurinCacheInvalidator invalidator = cluster.contexts().get(2).getBean(KurinCacheInvalidator.class);
        invalidator.evictAll("accounts").get(10, TimeUnit.SECONDS);

        // Applied on the instance that asked by the time the future completes
        assertThat(caches("accounts").get(2).get("a1").isHit()).isFalse();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(caches("accounts")).allSatisfy(cache -> assertThat(cache.get("a1").isHit()).isFalse()));
    }

    // Spec test 21, cluster part
    @Test
    void clusterStampedeProtectionLoadsOnceForThreeInstancesTimesOneHundredThreads() throws Exception {
        List<Catalog> instances = cluster.beans(Catalog.class);
        int threadsPerInstance = 100;
        callers = Executors.newFixedThreadPool(instances.size() * threadsPerInstance);
        CountDownLatch start = new CountDownLatch(1);

        List<Future<String>> results = new ArrayList<>();
        for (Catalog catalog : instances) {
            for (int i = 0; i < threadsPerInstance; i++) {
                results.add(callers.submit(() -> {
                    start.await();
                    return catalog.describe("item-7");
                }));
            }
        }
        start.countDown();

        for (Future<String> result : results) {
            assertThat(result.get(60, TimeUnit.SECONDS)).isEqualTo("description of item-7");
        }
        assertThat(Shared.CATALOG_LOADS.get()).isEqualTo(1);
        assertThat(Shared.store.contains("kurin:catalog:item-7")).isTrue();
    }

    @Test
    void cacheAnnotationsOnCommandHandlersAreAppliedOnEveryInstance() throws Exception {
        KurinTemplate template = cluster.contexts().get(1).getBean(KurinTemplate.class);

        template.submitCommand(new SetPrice("sku-1", new BigDecimal("9.99"))).get(10, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(caches("prices")).allSatisfy(cache -> {
                    assertThat(cache.get("sku-1").isHit()).isTrue();
                    assertThat(cache.get("sku-1").entry().value()).isEqualTo(new BigDecimal("9.99"));
                }));
        // Every instance built the entry from the time of the command, not from its own clock
        assertThat(caches("prices").stream().map(cache -> cache.get("sku-1").entry().createdAtMs()).distinct()).hasSize(1);

        template.submitCommand(new RemovePrice("sku-1")).get(10, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(caches("prices")).allSatisfy(cache -> assertThat(cache.get("sku-1").isHit()).isFalse()));
    }
}
