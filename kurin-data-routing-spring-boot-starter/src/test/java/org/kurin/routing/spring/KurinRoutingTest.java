package org.kurin.routing.spring;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kurin.routing.KurinReadOnly;
import org.kurin.routing.KurinRoutingContext;
import org.kurin.routing.KurinShardKey;
import org.kurin.routing.KurinShardRouter;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KurinRoutingTest {

    public record OrderDto(@KurinShardKey String customerId, double amount) {
    }

    public static class OrderRepository {
        private final KurinRoutingDataSource dataSource;
        private final Map<Connection, DataSource> owners;

        public OrderRepository(KurinRoutingDataSource dataSource, Map<Connection, DataSource> owners) {
            this.dataSource = dataSource;
            this.owners = owners;
        }

        public DataSource save(@KurinShardKey OrderDto order) throws SQLException {
            return owners.get(dataSource.getConnection());
        }

        @KurinReadOnly
        public DataSource findByCustomer(@KurinShardKey String customerId) throws SQLException {
            return owners.get(dataSource.getConnection());
        }

        public DataSource findAll() throws SQLException {
            return owners.get(dataSource.getConnection());
        }
    }

    private final Map<Connection, DataSource> owners = new HashMap<>();
    private final DataSource primaryA = pool();
    private final DataSource replicaA = pool();
    private final DataSource primaryB = pool();
    private final DataSource replicaB = pool();

    private String currentClient = "client-1";

    private DataSource pool() {
        Connection connection = stub(Connection.class, null);
        DataSource dataSource = stub(DataSource.class, connection);
        owners.put(connection, dataSource);
        return dataSource;
    }

    // Hand-made stub: every method except the Object ones returns the given connection
    private static <T> T stub(Class<T> type, Connection connection) {
        Object identity = new Object();
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) ->
                switch (method.getName()) {
                    case "hashCode" -> identity.hashCode();
                    case "equals" -> proxy == args[0];
                    case "toString" -> type.getSimpleName() + "@" + identity.hashCode();
                    default -> connection;
                }));
    }

    @AfterEach
    void clearContext() {
        KurinRoutingContext.clear();
        KurinRoutingContext.clearShard();
    }

    private OrderRepository repository(KurinDataSourceTopology topology) {
        SessionStickyManager stickyManager = new SessionStickyManager(60_000);
        KurinClientIdResolver clientIdResolver = () -> currentClient;
        KurinShardRouter shardRouter = new KurinShardRouter(topology.shards().keySet(), 160);

        KurinRoutingDataSource dataSource = new KurinRoutingDataSource(topology, stickyManager, clientIdResolver);
        dataSource.afterPropertiesSet();

        AspectJProxyFactory factory = new AspectJProxyFactory(new OrderRepository(dataSource, owners));
        factory.addAspect(new KurinRoutingAspect(stickyManager, clientIdResolver, shardRouter));
        return factory.getProxy();
    }

    @Test
    void readOnlyGoesToReplicaUntilTheClientWrites() throws SQLException {
        OrderRepository repository = repository(KurinDataSourceTopology.of(primaryA, replicaA));

        assertSame(replicaA, repository.findByCustomer("42"));
        assertSame(primaryA, repository.save(new OrderDto("42", 10.0)));
        assertSame(primaryA, repository.findByCustomer("42"), "read after write must stick to primary");

        currentClient = "client-2";
        assertSame(replicaA, repository.findByCustomer("42"), "other clients keep reading from replica");
    }

    @Test
    void shardKeyRoutesDtoAndPlainKeyToTheSameShard() throws SQLException {
        OrderRepository repository = repository(KurinDataSourceTopology.builder()
                .shard("shardA", primaryA, replicaA)
                .shard("shardB", primaryB, replicaB)
                .build());
        Map<DataSource, DataSource> replicaOf = Map.of(primaryA, replicaA, primaryB, replicaB);

        Set<DataSource> usedPrimaries = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            String customerId = "customer-" + i;
            currentClient = "writer-" + i;
            DataSource primary = repository.save(new OrderDto(customerId, i));
            usedPrimaries.add(primary);

            currentClient = "reader-" + i;
            assertSame(replicaOf.get(primary), repository.findByCustomer(customerId));
        }
        assertEquals(Set.of(primaryA, primaryB), usedPrimaries);
    }

    @Test
    void shardedTopologyRejectsOperationsWithoutShardKey() {
        OrderRepository repository = repository(KurinDataSourceTopology.builder()
                .shard("shardA", primaryA, replicaA)
                .shard("shardB", primaryB, replicaB)
                .build());

        assertThrows(IllegalStateException.class, repository::findAll);
    }

    @Test
    void consistentHashingMovesOnlyPartOfTheKeysWhenShardIsAdded() {
        KurinShardRouter twoShards = new KurinShardRouter(List.of("shardA", "shardB"), 160);
        KurinShardRouter threeShards = new KurinShardRouter(List.of("shardA", "shardB", "shardC"), 160);

        Map<String, Integer> distribution = new HashMap<>();
        int moved = 0;
        int total = 10_000;
        for (int i = 0; i < total; i++) {
            String before = twoShards.route("key-" + i);
            String after = threeShards.route("key-" + i);
            distribution.merge(before, 1, Integer::sum);
            if (!before.equals(after)) {
                assertEquals("shardC", after, "a key may only move to the new shard");
                moved++;
            }
        }

        assertTrue(distribution.get("shardA") > total * 0.35 && distribution.get("shardB") > total * 0.35,
                "uneven distribution: " + distribution);
        assertTrue(moved > total * 0.2 && moved < total * 0.5, "moved keys: " + moved);
    }
}
