package org.kurin.routing.spring;

import org.junit.jupiter.api.Test;
import org.kurin.routing.DataSourceType;
import org.kurin.routing.KurinCrossShardTransactionException;
import org.kurin.routing.KurinDataSourceTopology;
import org.kurin.routing.KurinPrimary;
import org.kurin.routing.KurinPrimaryUnavailableException;
import org.kurin.routing.KurinReadOnly;
import org.kurin.routing.KurinRoutingException;
import org.kurin.routing.KurinShardKey;
import org.kurin.routing.KurinSharded;
import org.kurin.routing.health.DataSourceHealthMonitor;
import org.kurin.routing.shard.KurinShardOperations;
import org.kurin.routing.shard.KurinShardRouter;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Routing against real JDBC: every node is its own H2 database holding a marker row with its name, so a query
 * tells which node answered. The databases are not replicated, which also shows where a write really went.
 */
class RoutingIntegrationTest {

    /**
     * A database that can be switched off, like a server that stopped accepting connections.
     */
    static final class Node extends AbstractDataSource {
        final String name;
        private final DriverManagerDataSource delegate;
        volatile boolean available = true;

        Node(String name, String database) {
            this.name = name;
            this.delegate = new DriverManagerDataSource("jdbc:h2:mem:" + database + "-" + name + ";DB_CLOSE_DELAY=-1");
            JdbcTemplate jdbc = new JdbcTemplate(delegate);
            jdbc.execute("create table if not exists marker (node varchar(32))");
            jdbc.execute("create table if not exists items (name varchar(64))");
            if (jdbc.queryForObject("select count(*) from marker", Integer.class) == 0) {
                jdbc.update("insert into marker values (?)", name);
            }
        }

        @Override
        public Connection getConnection() throws SQLException {
            if (!available) {
                throw new SQLException("Connection refused by " + name);
            }
            return delegate.getConnection();
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return getConnection();
        }

        List<String> items() {
            return new JdbcTemplate(delegate).queryForList("select name from items order by name", String.class);
        }
    }

    /**
     * The databases of one test, shared by the application instances (contexts) of that test.
     */
    static final class Databases {
        final Map<String, Node> nodes = new HashMap<>();
        final String id = UUID.randomUUID().toString();

        Node node(String name) {
            return nodes.computeIfAbsent(name, n -> new Node(n, id));
        }

        KurinDataSourceTopology single() {
            return KurinDataSourceTopology.of(node("primary"), node("replica"));
        }

        KurinDataSourceTopology sharded() {
            return KurinDataSourceTopology.builder()
                    .shard("A", node("A-primary"), node("A-replica"))
                    .shard("B", node("B-primary"), node("B-replica"))
                    .build();
        }
    }

    public static class Items {
        private final JdbcTemplate jdbc;
        private final ApplicationContext context;

        public Items(JdbcTemplate jdbc, ApplicationContext context) {
            this.jdbc = jdbc;
            this.context = context;
        }

        private String node() {
            return jdbc.queryForObject("select node from marker", String.class);
        }

        public String unmarked() {
            return node();
        }

        @KurinReadOnly
        public String read() {
            return node();
        }

        @KurinReadOnly(consistency = KurinReadOnly.Consistency.EVENTUAL)
        public String eventualRead() {
            return node();
        }

        @KurinPrimary
        @KurinReadOnly
        public String strictRead() {
            return node();
        }

        @Transactional(readOnly = true)
        public String readOnlyTransaction() {
            return node();
        }

        @Transactional
        public String write(String name) {
            jdbc.update("insert into items values (?)", name);
            return node();
        }

        @KurinReadOnly
        @Transactional
        public String writeInsideReadOnly(String name) {
            jdbc.update("insert into items values (?)", name);
            return node();
        }

        @KurinReadOnly(consistency = KurinReadOnly.Consistency.EVENTUAL)
        public String shardOf(@KurinShardKey String customerId) {
            return node();
        }

        @KurinSharded(key = "#customer.toLowerCase()")
        public String shardOfExpression(String customer) {
            return node();
        }

        @Transactional
        public String writeForCustomer(@KurinShardKey String customerId, String name) {
            jdbc.update("insert into items values (?)", name);
            return node();
        }

        @Transactional
        public void moveBetweenCustomers(@KurinShardKey String from, String to, String name) {
            jdbc.update("insert into items values (?)", name);
            // Goes through the proxy, so the shard key of the nested call is evaluated
            context.getBean(Items.class).writeForCustomer(to, name);
        }
    }

    public static class Events implements ApplicationListener<KurinNodeStateEvent> {
        final List<String> seen = new CopyOnWriteArrayList<>();

        @Override
        public void onApplicationEvent(KurinNodeStateEvent event) {
            seen.add(event.getClass().getSimpleName() + ":" + event.getShard());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class App {
        @Bean
        Items items(JdbcTemplate jdbc, ApplicationContext context) {
            return new Items(jdbc, context);
        }

        @Bean
        Events events() {
            return new Events();
        }
    }

    /**
     * The autoconfigurations of Spring Boot that the tests rely on moved between Spring Boot 3 and 4. The tests
     * run on both, so the class is looked up under its old and its new name.
     *
     * @param boot3 name below {@code org.springframework.boot.autoconfigure} in Spring Boot 3
     * @param boot4 name below {@code org.springframework.boot} in Spring Boot 4
     */
    static Class<?> boot(String boot3, String boot4) {
        for (String name : List.of("org.springframework.boot.autoconfigure." + boot3, "org.springframework.boot." + boot4)) {
            if (org.springframework.util.ClassUtils.isPresent(name, RoutingIntegrationTest.class.getClassLoader())) {
                return org.springframework.util.ClassUtils.resolveClassName(name, RoutingIntegrationTest.class.getClassLoader());
            }
        }
        throw new IllegalStateException("Neither " + boot3 + " nor " + boot4 + " is on the classpath");
    }

    private ApplicationContextRunner runner(KurinDataSourceTopology topology) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KurinRoutingAutoConfiguration.class, AopAutoConfiguration.class,
                        boot("jdbc.DataSourceAutoConfiguration", "jdbc.autoconfigure.DataSourceAutoConfiguration"),
                        boot("jdbc.DataSourceTransactionManagerAutoConfiguration", "jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration"),
                        boot("jdbc.JdbcTemplateAutoConfiguration", "jdbc.autoconfigure.JdbcTemplateAutoConfiguration"),
                        boot("transaction.TransactionAutoConfiguration", "transaction.autoconfigure.TransactionAutoConfiguration")))
                .withBean(KurinDataSourceTopology.class, () -> topology)
                .withUserConfiguration(App.class)
                // The tests drive the health checks themselves
                // A context made by this runner is never declared ready, so the mode that depends on it is not used here
                .withPropertyValues("kurin.routing.unrouted.mode=fail",
                        "kurin.routing.health.interval=1h", "kurin.routing.health.recovery-threshold=2",
                        "kurin.routing.token-secret=test-secret", "kurin.routing.sticky-window=400ms");
    }

    @Test
    void annotationsAndTransactionsSelectPrimaryOrReplica() {
        Databases databases = new Databases();
        runner(databases.single()).run(context -> {
            assertThat(context).hasNotFailed();
            Items items = context.getBean(Items.class);

            assertThat(items.unmarked()).isEqualTo("primary");
            assertThat(items.read()).isEqualTo("replica");
            assertThat(items.eventualRead()).isEqualTo("replica");
            assertThat(items.strictRead()).isEqualTo("primary");
            assertThat(items.write("first")).isEqualTo("primary");
            // Without the opt-in a read-only transaction says nothing about replicas
            assertThat(items.readOnlyTransaction()).isEqualTo("primary");
        });
        // This thread has just written, which keeps its reads on the primary for the sticky window. The window
        // is made too short to matter, so that the outcome does not depend on how long the context takes to start.
        runner(databases.single())
                .withPropertyValues("kurin.routing.readonly-transactions-to-replica=true", "kurin.routing.sticky-window=1ms")
                .run(context -> assertThat(context.getBean(Items.class).readOnlyTransaction()).isEqualTo("replica"));
    }

    // Spec test 25
    @Test
    void writeInsideReadOnlyMethodGoesToThePrimary() {
        Databases databases = new Databases();
        runner(databases.single()).run(context -> {
            Items items = context.getBean(Items.class);

            assertThat(items.writeInsideReadOnly("misplaced?")).isEqualTo("primary");

            assertThat(databases.node("primary").items()).containsExactly("misplaced?");
            assertThat(databases.node("replica").items()).isEmpty();
        });
    }

    /**
     * Runs the action as one HTTP request through the consistency filter of an instance.
     *
     * @return the token the instance sent back, or null
     */
    private static String request(ApplicationContext instance, String token, AtomicReference<String> answer,
                                  java.util.function.Function<Items, String> action) throws Exception {
        KurinRoutingProperties props = instance.getBean(KurinRoutingProperties.class);
        KurinConsistencyFilter filter = new KurinConsistencyFilter(
                new ServletConsistencyTokenCarrier(props.getToken().getHeaderName(), "kurin-consistency"), props.getTokenSecret());
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/items");
        if (token != null) {
            request.addHeader("X-Kurin-Consistency", token);
        }
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, (req, res) -> {
            answer.set(action.apply(instance.getBean(Items.class)));
            res.getWriter().write("done");
        });
        if (response.getHeader("X-Kurin-Consistency") != null) {
            assertThat(response.getCookie("kurin-consistency").getValue()).isEqualTo(response.getHeader("X-Kurin-Consistency"));
            assertThat(response.getCookie("kurin-consistency").isHttpOnly()).isTrue();
        }
        return response.getHeader("X-Kurin-Consistency");
    }

    // Spec test 26
    @Test
    void readYourWritesHoldsAcrossTwoInstancesThroughTheToken() {
        Databases databases = new Databases();
        // Two application instances behind a load balancer: separate contexts, the same databases
        // The window is longer than any pause of the JVM between the write and the reads that must fall inside it
        ApplicationContextRunner instance = runner(databases.single()).withPropertyValues("kurin.routing.sticky-window=3s");
        instance.run(first -> instance.run(second -> {
            AtomicReference<String> answer = new AtomicReference<>();

            // A client that has not written reads from the replica and gets no token
            assertThat(request(second, null, answer, Items::read)).isNull();
            assertThat(answer.get()).isEqualTo("replica");

            // It writes through the first instance, and receives a token
            String token = request(first, null, answer, items -> items.write("order-1"));
            assertThat(answer.get()).isEqualTo("primary");
            assertThat(token).isNotBlank();

            // Its next request lands on the second instance, which has never seen this client
            assertThat(request(second, token, answer, Items::read)).isNull();
            assertThat(answer.get()).isEqualTo("primary");
            // An eventual read stays on the replica even so, and other clients are unaffected
            request(second, token, answer, Items::eventualRead);
            assertThat(answer.get()).isEqualTo("replica");
            request(second, null, answer, Items::read);
            assertThat(answer.get()).isEqualTo("replica");

            // A forged token is ignored: the request is served as if it carried none
            request(second, token.substring(0, token.length() - 4) + "0000", answer, Items::read);
            assertThat(answer.get()).isEqualTo("replica");

            // After the sticky window the replica is trusted again
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                request(second, token, answer, Items::read);
                assertThat(answer.get()).isEqualTo("replica");
            });
        }));
    }

    // Spec test 27
    @Test
    void deadReplicaFallsBackToPrimaryAndDeadPrimaryFailsWritesButNotEventualReads() {
        Databases databases = new Databases();
        runner(databases.single()).run(context -> {
            Items items = context.getBean(Items.class);
            DataSourceHealthMonitor monitor = context.getBean(DataSourceHealthMonitor.class);
            Events events = context.getBean(Events.class);

            databases.node("replica").available = false;
            // The very read that hits the dead replica is answered by the primary
            assertThat(items.read()).isEqualTo("primary");
            assertThat(items.eventualRead()).isEqualTo("primary");
            assertThat(monitor.isUp(KurinDataSourceTopology.DEFAULT_SHARD, DataSourceType.REPLICA)).isFalse();
            assertThat(events.seen).containsExactly("KurinReplicaDownEvent:default");

            databases.node("replica").available = true;
            monitor.checkAll();
            assertThat(items.read()).isEqualTo("primary");
            monitor.checkAll();
            assertThat(items.read()).isEqualTo("replica");

            databases.node("primary").available = false;
            assertThatThrownBy(() -> items.write("lost")).isInstanceOf(KurinPrimaryUnavailableException.class);
            // From now on the failure is immediate, without another connection attempt
            assertThatThrownBy(() -> items.write("lost")).isInstanceOf(KurinPrimaryUnavailableException.class);
            assertThatThrownBy(items::strictRead).isInstanceOf(KurinPrimaryUnavailableException.class);
            assertThatThrownBy(items::unmarked).isInstanceOf(KurinPrimaryUnavailableException.class);
            // Read-only mode: replica reads keep working
            assertThat(items.eventualRead()).isEqualTo("replica");
            assertThat(items.read()).isEqualTo("replica");

            databases.node("primary").available = true;
            monitor.checkAll();
            monitor.checkAll();
            assertThat(items.write("recovered")).isEqualTo("primary");
            assertThat(databases.node("primary").items()).containsExactly("recovered");
            assertThat(events.seen).containsExactly("KurinReplicaDownEvent:default", "KurinReplicaUpEvent:default",
                    "KurinPrimaryDownEvent:default", "KurinPrimaryUpEvent:default");
        });
    }

    @Test
    void replicaFallbackCanBeTurnedIntoAFailure() {
        Databases databases = new Databases();
        runner(databases.single()).withPropertyValues("kurin.routing.replica-fallback=fail").run(context -> {
            Items items = context.getBean(Items.class);
            databases.node("replica").available = false;

            assertThatThrownBy(items::read).isInstanceOf(Exception.class);
            assertThatThrownBy(items::read).isInstanceOf(KurinRoutingException.class).hasMessageContaining("replica");
            assertThat(items.unmarked()).isEqualTo("primary");
        });
    }

    @Test
    void shardKeysSelectTheShardAndUnkeyedAccessIsRefused() {
        Databases databases = new Databases();
        runner(databases.sharded()).run(context -> {
            assertThat(context).hasNotFailed();
            Items items = context.getBean(Items.class);
            KurinShardRouter router = context.getBean(KurinShardRouter.class);

            for (int i = 0; i < 50; i++) {
                String customer = "customer-" + i;
                String shard = router.route(customer);
                assertThat(items.shardOf(customer)).isEqualTo(shard + "-replica");
                assertThat(items.shardOfExpression(customer.toUpperCase())).isEqualTo(shard + "-primary");
                assertThat(items.writeForCustomer(customer, customer)).isEqualTo(shard + "-primary");
            }
            assertThat(databases.node("A-primary").items()).isNotEmpty();
            assertThat(databases.node("B-primary").items()).isNotEmpty();
            assertThat(databases.node("A-primary").items()).doesNotContainAnyElementsOf(databases.node("B-primary").items());

            assertThatThrownBy(items::unmarked).isInstanceOf(KurinRoutingException.class).hasMessageContaining("No shard is selected");

            KurinShardOperations operations = context.getBean(KurinShardOperations.class);
            assertThat(operations.executeOnAllShards(shard -> items.eventualRead()).values())
                    .containsExactlyInAnyOrder("A-replica", "B-replica");
        });
    }

    // 1.1.1, section 5.2
    @Test
    void fanOutIsObservedWithAChildPerShardInTheTraceOfTheCaller() {
        record Observed(String name, String tags, io.micrometer.observation.ObservationView parent, boolean failed) {
        }
        List<Observed> observed = new CopyOnWriteArrayList<>();
        io.micrometer.observation.ObservationRegistry registry = io.micrometer.observation.ObservationRegistry.create();
        registry.observationConfig().observationHandler(new io.micrometer.observation.ObservationHandler<io.micrometer.observation.Observation.Context>() {
            @Override
            public void onStop(io.micrometer.observation.Observation.Context context) {
                observed.add(new Observed(context.getName(), context.getLowCardinalityKeyValues().toString(),
                        context.getParentObservation(), context.getError() != null));
            }

            @Override
            public boolean supportsContext(io.micrometer.observation.Observation.Context context) {
                return true;
            }
        });

        Databases databases = new Databases();
        runner(databases.sharded())
                .withConfiguration(AutoConfigurations.of(KurinRoutingObservabilityAutoConfiguration.class))
                .withBean(io.micrometer.observation.ObservationRegistry.class, () -> registry)
                .run(context -> {
                    KurinShardOperations operations = context.getBean(KurinShardOperations.class);
                    Items items = context.getBean(Items.class);

                    io.micrometer.observation.Observation request = io.micrometer.observation.Observation.start("request", registry);
                    AtomicReference<io.micrometer.observation.Observation> seenInTask = new AtomicReference<>();
                    try (io.micrometer.observation.Observation.Scope scope = request.openScope()) {
                        operations.executeOnAllShards(shard -> {
                            seenInTask.set(registry.getCurrentObservation());
                            return items.eventualRead();
                        });
                    } finally {
                        request.stop();
                    }

                    Observed fanOut = observed.stream().filter(o -> o.name().equals("kurin.routing.fanout")).findFirst().orElseThrow();
                    assertThat(fanOut.tags()).contains("shards", "2").contains("failed", "0");
                    // The fan-out belongs to the trace of whoever started it
                    assertThat(fanOut.parent().getContextView().getName()).isEqualTo("request");
                    List<Observed> perShard = observed.stream().filter(o -> o.name().equals("kurin.routing.fanout.shard")).toList();
                    assertThat(perShard).hasSize(2).allSatisfy(shard -> {
                        assertThat(shard.parent().getContextView().getName()).isEqualTo("kurin.routing.fanout");
                        assertThat(shard.failed()).isFalse();
                    });
                    assertThat(perShard.stream().map(Observed::tags).toList().toString()).contains("A").contains("B");
                    // Inside the callback the span of the shard is current, on the pool thread
                    assertThat(seenInTask.get().getContextView().getName()).isEqualTo("kurin.routing.fanout.shard");

                    // A failing shard is counted on the fan-out and marked on its own span
                    observed.clear();
                    operations.executeOnAllShards(shard -> {
                        if (shard.equals("B")) {
                            throw new IllegalStateException("shard B is broken");
                        }
                        return shard;
                    }, KurinShardOperations.FanOutOptions.defaults().failurePolicy(KurinShardOperations.FailurePolicy.COLLECT));
                    assertThat(observed.stream().filter(o -> o.name().equals("kurin.routing.fanout")).findFirst().orElseThrow().tags())
                            .contains("failed", "1");
                    assertThat(observed.stream().filter(o -> o.name().equals("kurin.routing.fanout.shard")).filter(Observed::failed)).hasSize(1);
                });
    }

    // Spec test 29
    @Test
    void secondShardInsideATransactionIsRefused() {
        Databases databases = new Databases();
        runner(databases.sharded()).run(context -> {
            Items items = context.getBean(Items.class);
            KurinShardRouter router = context.getBean(KurinShardRouter.class);
            String onA = customerOn(router, "A");
            String onB = customerOn(router, "B");

            assertThatThrownBy(() -> items.moveBetweenCustomers(onA, onB, "transfer"))
                    .isInstanceOf(KurinCrossShardTransactionException.class)
                    .hasMessageContaining("'A'")
                    .hasMessageContaining("'B'");
            // The transaction rolled back: nothing was written anywhere
            assertThat(databases.node("A-primary").items()).isEmpty();
            assertThat(databases.node("B-primary").items()).isEmpty();

            // Two keys of the same shard are fine, and the guard does not outlive the transaction
            String alsoOnA = customerOn(router, "A", onA);
            items.moveBetweenCustomers(onA, alsoOnA, "same-shard");
            assertThat(databases.node("A-primary").items()).containsExactly("same-shard", "same-shard");
            assertThat(items.writeForCustomer(onB, "later")).isEqualTo("B-primary");
        });
    }

    private static String customerOn(KurinShardRouter router, String shard, String... except) {
        for (int i = 0; i < 10_000; i++) {
            String customer = "customer-" + i;
            if (router.route(customer).equals(shard) && !List.of(except).contains(customer)) {
                return customer;
            }
        }
        throw new IllegalStateException("No key found for shard " + shard);
    }

    @Test
    void shardWeightFromPropertiesOverridesTheTopology() {
        Databases databases = new Databases();
        runner(databases.sharded()).withPropertyValues("kurin.routing.shards.B.weight=4").run(context -> {
            KurinShardRouter router = context.getBean(KurinShardRouter.class);
            int onB = 0;
            for (int i = 0; i < 20_000; i++) {
                if (router.route("customer-" + i).equals("B")) {
                    onB++;
                }
            }
            assertThat(onB / 20_000.0).isBetween(0.75, 0.85);
        });
        runner(databases.sharded()).withPropertyValues("kurin.routing.shards.C.weight=4").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).rootCause().hasMessageContaining("kurin.routing.shards.C");
        });
    }
}
