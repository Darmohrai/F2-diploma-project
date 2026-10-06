package org.kurin.kurincachespringbootstarter;

import org.kurin.api.KurinCommand;
import org.kurin.kurintieredcache.api.CacheKeyProvider;
import org.kurin.kurintieredcache.api.CacheTier;
import org.kurin.kurintieredcache.api.KurinCacheEvict;
import org.kurin.kurintieredcache.api.KurinCachePut;
import org.kurin.kurintieredcache.core.*;
import org.kurin.kurintieredcache.provider.CaffeineCacheProvider;
import org.kurin.kurintieredcache.provider.RocksDBCacheProvider;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.raft.KurinNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class KurinCachePlayground {

    // 1. DTO Команди (Реалізують CacheKeyProvider, щоб кеш знав ключ)
    public record UpdateProductCommand(String productId, String data) implements CacheKeyProvider {
        @Override
        public String getCacheKey() { return productId; }
    }

    public record DeleteProductCommand(String productId) implements CacheKeyProvider {
        @Override
        public String getCacheKey() { return productId; }
    }

    // 2. Бізнес-сервіс з анотаціями Кешу
    public static class ProductService {
        private final Map<String, String> db = new HashMap<>();

        @KurinCommand
        @KurinCachePut(cacheName = "products", tier = CacheTier.TIERED_SMART)
        public String updateProduct(UpdateProductCommand cmd) {
            System.out.println("[DB-Service] Збереження '" + cmd.productId() + "' в БД...");
            db.put(cmd.productId(), cmd.data());
            return cmd.data(); // Результат піде в кеш!
        }

        @KurinCommand
        @KurinCacheEvict(cacheName = "products")
        public String deleteProduct(DeleteProductCommand cmd) {
            System.out.println("[DB-Service] Видалення '" + cmd.productId() + "' з БД...");
            db.remove(cmd.productId());
            return "DELETED";
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== Ініціалізація кластера з 3-х нод (Smart Cache + Raft) ===");

        // Ініціалізуємо менеджер кешу (будемо слухати кеш на Ноді 1)
        KryoSerializer serializer = new KryoSerializer(List.of(UpdateProductCommand.class, DeleteProductCommand.class));
        CacheConfigurationContext context = new CacheConfigurationContext(serializer, "./kurin-data/playground-cache");
        KurinCacheManager cacheManager = new DefaultKurinCacheManager(
                List.of(new CaffeineCacheProvider(), new RocksDBCacheProvider()), context
        );

        ProductService service1 = new ProductService();
        ProductService service2 = new ProductService();
        ProductService service3 = new ProductService();

        // Updater перехоплює коміти Raft і оновлює кеш
        RaftCacheUpdater cacheUpdater1 = new RaftCacheUpdater(cacheManager, List.of(service1));

        KurinNode node1 = KurinNode.builder().localNode("127.0.0.1", 9021).addPeers("127.0.0.1:9022", "127.0.0.1:9023")
                .addService(service1).registerCommand(UpdateProductCommand.class).registerCommand(DeleteProductCommand.class)
                .addStateMachineListener(cacheUpdater1) // <-- Підключаємо слухача до Ноди 1
                .build();

        KurinNode node2 = KurinNode.builder().localNode("127.0.0.1", 9022).addPeers("127.0.0.1:9021", "127.0.0.1:9023")
                .addService(service2).registerCommand(UpdateProductCommand.class).registerCommand(DeleteProductCommand.class).build();

        KurinNode node3 = KurinNode.builder().localNode("127.0.0.1", 9023).addPeers("127.0.0.1:9021", "127.0.0.1:9022")
                .addService(service3).registerCommand(UpdateProductCommand.class).registerCommand(DeleteProductCommand.class).build();

        node1.start(); node2.start(); node3.start();

        System.out.println("\n[Кластер] Чекаємо 3 секунди на вибори лідера...");
        Thread.sleep(3000);

        System.out.println("\n=== Тестування розподіленого кешування ===");
        KurinCache cache = cacheManager.getOrCreateCache("products", CacheTier.TIERED_SMART);

        System.out.println("1. Читаємо локальний кеш Ноди 1 для 'PROD-99': " + cache.get("PROD-99"));

        System.out.println("\n2. Відправляємо команду оновлення ЧЕРЕЗ НОДУ 2...");
        node2.submitCommand(new UpdateProductCommand("PROD-99", "Квадрокоптер DJI Mavic 3")).get(3, TimeUnit.SECONDS);

        Thread.sleep(500); // Чекаємо мілісекунди, поки StateMachineListener оновить кеш

        System.out.println("\n3. Перевіряємо локальний кеш Ноди 1 знову: " + cache.get("PROD-99"));

        System.out.println("\n4. Відправляємо команду видалення ЧЕРЕЗ НОДУ 3...");
        node3.submitCommand(new DeleteProductCommand("PROD-99")).get(3, TimeUnit.SECONDS);

        Thread.sleep(500);

        System.out.println("\n5. Перевіряємо кеш Ноди 1 після видалення: " + cache.get("PROD-99"));

        System.out.println("\n=== Тест завершено! Зупиняємо кластер... ===");
        node1.shutdown(); node2.shutdown(); node3.shutdown();
        cacheManager.close();
        System.exit(0);
    }
}