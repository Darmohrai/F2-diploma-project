package org.kurin.kurincachespringbootstarter;

import org.junit.jupiter.api.Test;
import org.kurin.api.KurinCommand;
import org.kurin.kurintieredcache.api.CacheKeyProvider;
import org.kurin.kurintieredcache.api.CacheTier;
import org.kurin.kurintieredcache.api.KurinCachePut;
import org.kurin.kurintieredcache.api.KurinCacheable;
import org.kurin.kurintieredcache.core.KurinCacheManager;
import org.kurin.raft.KurinNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(properties = {
        "kurin.node.host=127.0.0.1",
        "kurin.node.port=9021",
        "kurin.node.peers="
})
class CacheIntegrationTest {

    @SpringBootApplication
    static class TestConfig {
        @Bean
        public ProductService productService() {
            return new ProductService();
        }
    }

    public record UpdatePriceCommand(String productId, double newPrice) implements CacheKeyProvider {
        @Override
        public String getCacheKey() {
            return productId;
        }
    }

    public static class ProductService {
        private final Map<String, Double> database = new HashMap<>();
        private int dbReadCount = 0; // Зробили приватним, щоб точно не помилитися

        public ProductService() {
            database.put("PROD_1", 100.0);
        }

        @KurinCacheable(cacheName = "products", tier = CacheTier.TIERED_SMART, key = "PROD_1")
        public double getPrice(String productId) {
            dbReadCount++;
            System.out.println(">>> Реальне звернення до БД для: " + productId);
            return database.getOrDefault(productId, 0.0);
        }

        @KurinCommand
        @KurinCachePut(cacheName = "products", tier = CacheTier.TIERED_SMART)
        public double updatePrice(UpdatePriceCommand cmd) {
            System.out.println(">>> Зміна стану в Raft: " + cmd);
            database.put(cmd.productId(), cmd.newPrice());
            return cmd.newPrice();
        }

        public int getDbReadCount() {
            return dbReadCount;
        }
    }

    @Autowired
    private KurinNode kurinNode;

    @Autowired
    private ProductService productService;

    @Autowired
    private KurinCacheManager cacheManager;

    @Test
    void shouldCacheReadsAndProcessRaftUpdates() throws Exception {
        Thread.sleep(1000);

        System.out.println("--- Читання 1 (має бути Cache MISS) ---");
        double price1 = productService.getPrice("PROD_1");
        assertEquals(100.0, price1);
        assertEquals(1, productService.getDbReadCount()); // Виправлено!

        System.out.println("--- Читання 2 (має бути Cache HIT) ---");
        double price2 = productService.getPrice("PROD_1");
        assertEquals(100.0, price2);
        assertEquals(1, productService.getDbReadCount()); // Виправлено!

        System.out.println("--- Зміна ціни через Raft ---");
        UpdatePriceCommand cmd = new UpdatePriceCommand("PROD_1", 250.0);
        CompletableFuture<Object> future = kurinNode.submitCommand(cmd);
        future.get(3, TimeUnit.SECONDS);

        Thread.sleep(150); // Дамо кешу час на запис

        System.out.println("--- Читання 3 після Raft (має бути Cache HIT з НОВОЮ ціною) ---");
        double price3 = productService.getPrice("PROD_1");
        assertEquals(250.0, price3);
        assertEquals(1, productService.getDbReadCount());

        System.out.println("✅ Успіх! Smart Cache + Raft Consensus працюють ідеально.");
    }
}
