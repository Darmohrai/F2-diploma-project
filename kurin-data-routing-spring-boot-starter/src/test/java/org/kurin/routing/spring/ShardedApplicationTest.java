package org.kurin.routing.spring;

import jakarta.persistence.Entity;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Id;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Table;
import liquibase.integration.spring.SpringLiquibase;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kurin.routing.KurinDataSourceTopology;
import org.kurin.routing.KurinRoutingException;
import org.kurin.routing.KurinShardKey;
import org.kurin.routing.shard.KurinShardOperations;
import org.kurin.routing.shard.KurinShardRouter;
import org.kurin.routing.spring.migration.KurinShardedFlyway;
import org.kurin.routing.spring.migration.KurinShardedLiquibase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A whole application on two shards, started the way applications are started: JPA, a migration tool and the
 * routing data source as the only {@code DataSource}. Everything that touches the database before the
 * application can choose a shard has to work, and the schema has to arrive on every shard.
 * <p>
 * Every node is an H2 database of its own. The databases are not replicated, so a replica shows whether
 * something was run on it that should not have been.
 */
class ShardedApplicationTest {

    /**
     * The databases of one test, found by the application through an id in its properties.
     */
    static final class Databases {
        static final Map<String, Databases> ALL = new ConcurrentHashMap<>();

        final String id = "sharded-" + UUID.randomUUID();
        final Map<String, DataSource> nodes = new ConcurrentHashMap<>();

        Databases() {
            ALL.put(id, this);
        }

        DataSource node(String name) {
            return nodes.computeIfAbsent(name, n -> new DriverManagerDataSource("jdbc:h2:mem:" + id + "-" + n + ";DB_CLOSE_DELAY=-1"));
        }

        JdbcTemplate jdbc(String name) {
            return new JdbcTemplate(node(name));
        }

        boolean hasTable(String node, String table) {
            Integer count = jdbc(node).queryForObject(
                    "select count(*) from information_schema.tables where upper(table_name) = ?", Integer.class, table.toUpperCase());
            return count != null && count > 0;
        }

        List<String> customers(String node) {
            return jdbc(node).queryForList("select id from customers order by id", String.class);
        }
    }

    @Entity
    @Table(name = "customers")
    public static class Customer {
        @Id
        private String id;
        private String name;

        protected Customer() {
        }

        public Customer(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }

    public static class Customers {
        @PersistenceContext
        private EntityManager entities;

        @Transactional
        public void save(@KurinShardKey String id, String name) {
            entities.persist(new Customer(id, name));
        }

        @Transactional(readOnly = true)
        public String nameOf(@KurinShardKey String id) {
            Customer customer = entities.find(Customer.class, id);
            return customer != null ? customer.getName() : null;
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class App {
        @Bean
        KurinDataSourceTopology topology(@Value("${test.databases}") String id, @Value("${test.shards:2}") int shards) {
            Databases databases = Databases.ALL.get(id);
            if (shards == 1) {
                return KurinDataSourceTopology.of(databases.node("A-primary"), databases.node("A-replica"));
            }
            return KurinDataSourceTopology.builder()
                    .shard("A", databases.node("A-primary"), databases.node("A-replica"))
                    .shard("B", databases.node("B-primary"), databases.node("B-replica"))
                    .build();
        }

        @Bean
        Customers customers() {
            return new Customers();
        }
    }

    private final List<ConfigurableApplicationContext> started = new ArrayList<>();
    private final Databases databases = new Databases();

    @AfterEach
    void stopApplications() {
        started.forEach(ConfigurableApplicationContext::close);
        Databases.ALL.remove(databases.id);
    }

    private ConfigurableApplicationContext start(String... properties) {
        List<String> all = new ArrayList<>(List.of(
                "spring.main.banner-mode=off",
                "spring.jmx.enabled=false",
                "spring.jpa.open-in-view=false",
                "spring.jpa.hibernate.ddl-auto=validate",
                "kurin.routing.health.interval=1h",
                "test.databases=" + databases.id));
        all.addAll(List.of(properties));
        ConfigurableApplicationContext context = new SpringApplicationBuilder(App.class)
                .web(WebApplicationType.NONE)
                .properties(all.toArray(String[]::new))
                .run();
        started.add(context);
        return context;
    }

    private ConfigurableApplicationContext startWithLiquibase(String... properties) {
        List<String> all = new ArrayList<>(List.of("spring.flyway.enabled=false"));
        all.addAll(List.of(properties));
        return start(all.toArray(String[]::new));
    }

    // Two keys that are known to live on different shards
    private static String keyOn(KurinShardRouter router, String shard) {
        for (int i = 0; i < 1000; i++) {
            if (router.route("customer-" + i).equals(shard)) {
                return "customer-" + i;
            }
        }
        throw new IllegalStateException("No key found for shard " + shard);
    }

    @Test
    void applicationStartsOnTwoShardsAndTheSchemaIsOnEveryPrimaryOnly() {
        ConfigurableApplicationContext context = startWithLiquibase();

        // Migrations ran once per shard, on the primaries
        assertThat(context.getBean(SpringLiquibase.class)).isInstanceOf(KurinShardedLiquibase.class);
        assertThat(((KurinShardedLiquibase) context.getBean(SpringLiquibase.class)).shards()).containsExactly("A", "B");
        for (String primary : List.of("A-primary", "B-primary")) {
            assertThat(databases.hasTable(primary, "customers")).as(primary).isTrue();
            assertThat(databases.hasTable(primary, "databasechangelog")).as(primary).isTrue();
        }
        for (String replica : List.of("A-replica", "B-replica")) {
            assertThat(databases.hasTable(replica, "customers")).as(replica).isFalse();
            assertThat(databases.hasTable(replica, "databasechangelog")).as(replica).isFalse();
        }

        // JPA works per shard
        KurinShardRouter router = context.getBean(KurinShardRouter.class);
        Customers customers = context.getBean(Customers.class);
        String onA = keyOn(router, "A");
        String onB = keyOn(router, "B");
        customers.save(onA, "Ann");
        customers.save(onB, "Bob");
        assertThat(databases.customers("A-primary")).containsExactly(onA);
        assertThat(databases.customers("B-primary")).containsExactly(onB);
        assertThat(customers.nameOf(onB)).isEqualTo("Bob");

        // The start is over: access without a shard is refused again
        JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
        assertThatThrownBy(() -> jdbc.queryForObject("select count(*) from customers", Integer.class))
                .isInstanceOf(KurinRoutingException.class)
                .hasStackTraceContaining("No shard is selected")
                .hasStackTraceContaining("startup-only");
    }

    @Test
    void startupStateBelongsToTheApplicationNotToTheJvm() {
        ConfigurableApplicationContext first = startWithLiquibase();
        assertThat(first.getBean(KurinRoutingStartup.class).isFinished()).isTrue();

        // A second application in the same JVM starts while the first is long ready: its own start is still open
        ConfigurableApplicationContext second = startWithLiquibase();
        assertThat(second.getBean(KurinRoutingStartup.class).isFinished()).isTrue();
        assertThat(second.getBean(KurinRoutingStartup.class)).isNotSameAs(first.getBean(KurinRoutingStartup.class));
    }

    @Test
    void defaultShardModeServesAccessWithoutAShardForGood() {
        ConfigurableApplicationContext context = startWithLiquibase(
                "kurin.routing.unrouted.mode=default-shard", "kurin.routing.unrouted.shard=B");
        JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);

        jdbc.update("insert into customers (id, name) values (?, ?)", "global", "kept on the default shard");

        assertThat(databases.customers("B-primary")).containsExactly("global");
        assertThat(databases.customers("A-primary")).isEmpty();
        assertThat(jdbc.queryForObject("select name from customers where id = ?", String.class, "global"))
                .isEqualTo("kept on the default shard");
    }

    @Test
    void unknownUnroutedShardFailsTheStart() {
        assertThatThrownBy(() -> startWithLiquibase("kurin.routing.unrouted.shard=C"))
                .hasStackTraceContaining("kurin.routing.unrouted.shard=C")
                .hasStackTraceContaining("is not a shard of the topology");
    }

    @Test
    void failModeWithoutADialectFailsTheStartWithAnExplanation() {
        assertThatThrownBy(() -> startWithLiquibase("kurin.routing.unrouted.mode=fail", "spring.jpa.hibernate.ddl-auto=none"))
                .hasStackTraceContaining("kurin.routing.unrouted.mode=fail")
                .hasStackTraceContaining("spring.jpa.database-platform")
                .hasStackTraceContaining("startup-only");
    }

    @Test
    void failModeWithADialectStartsWithoutTouchingADatabaseOutsideOfAShard() {
        ConfigurableApplicationContext context = startWithLiquibase(
                "kurin.routing.unrouted.mode=fail",
                "spring.jpa.hibernate.ddl-auto=none",
                "spring.jpa.database-platform=org.hibernate.dialect.H2Dialect");

        // The migrations still reached every shard: they address the primaries, not the routing data source
        assertThat(databases.hasTable("A-primary", "customers")).isTrue();
        assertThat(databases.hasTable("B-primary", "customers")).isTrue();

        KurinShardRouter router = context.getBean(KurinShardRouter.class);
        Customers customers = context.getBean(Customers.class);
        String onB = keyOn(router, "B");
        customers.save(onB, "Bob");
        assertThat(customers.nameOf(onB)).isEqualTo("Bob");

        JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
        assertThatThrownBy(() -> jdbc.queryForObject("select count(*) from customers", Integer.class))
                .isInstanceOf(KurinRoutingException.class);
    }

    @Test
    void flywayMigrationsRunOnEveryPrimaryOnly() {
        ConfigurableApplicationContext context = start("spring.liquibase.enabled=false");

        assertThat(context.getBean(Flyway.class)).isInstanceOf(KurinShardedFlyway.class);
        assertThat(((KurinShardedFlyway) context.getBean(Flyway.class)).shards()).containsExactly("A", "B");
        for (String primary : List.of("A-primary", "B-primary")) {
            assertThat(databases.hasTable(primary, "customers")).as(primary).isTrue();
            assertThat(databases.hasTable(primary, "flyway_schema_history")).as(primary).isTrue();
        }
        for (String replica : List.of("A-replica", "B-replica")) {
            assertThat(databases.hasTable(replica, "customers")).as(replica).isFalse();
        }

        KurinShardRouter router = context.getBean(KurinShardRouter.class);
        Customers customers = context.getBean(Customers.class);
        String onA = keyOn(router, "A");
        customers.save(onA, "Ann");
        assertThat(databases.customers("A-primary")).containsExactly(onA);
    }

    @Test
    void migrationsCanBeLimitedToSomeShardsOrLeftToSpringBoot() {
        startWithLiquibase("spring.jpa.hibernate.ddl-auto=none", "kurin.routing.migrations.shards=B");
        assertThat(databases.hasTable("B-primary", "customers")).isTrue();
        assertThat(databases.hasTable("A-primary", "customers")).isFalse();
    }

    @Test
    void withMigrationsPerShardSwitchedOffSpringBootMigratesTheUnroutedShardAlone() {
        ConfigurableApplicationContext context = startWithLiquibase("kurin.routing.migrations.enabled=false");
        assertThat(context.getBean(SpringLiquibase.class)).isNotInstanceOf(KurinShardedLiquibase.class);
        assertThat(databases.hasTable("A-primary", "customers")).isTrue();
        assertThat(databases.hasTable("B-primary", "customers")).isFalse();
    }

    @Test
    void singleShardIsLeftToTheStandardMigration() {
        ConfigurableApplicationContext context = startWithLiquibase("test.shards=1");
        assertThat(context.getBean(SpringLiquibase.class)).isNotInstanceOf(KurinShardedLiquibase.class);
        assertThat(databases.hasTable("A-primary", "customers")).isTrue();
        assertThat(databases.hasTable("A-replica", "customers")).isFalse();
    }

    @Test
    void failedMigrationOfAShardFailsTheStartAndNamesTheShard() {
        // The change set cannot be applied on B: the table is already there, unknown to Liquibase
        databases.jdbc("B-primary").execute("create table customers (id varchar(64) primary key)");

        assertThatThrownBy(this::startWithLiquibase)
                .hasStackTraceContaining("Liquibase migration of shard 'B' failed");
        // Shards are migrated in the order of the topology: A was done before B failed
        assertThat(databases.hasTable("A-primary", "databasechangelog")).isTrue();
    }

    @Test
    void shardOperationsRunTransactionsOnTheShardTheyName() throws Exception {
        ConfigurableApplicationContext context = startWithLiquibase();
        KurinShardOperations shards = context.getBean(KurinShardOperations.class);
        JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
        TransactionTemplate transaction = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));

        // Maintenance of one shard, in a transaction that commits
        shards.executeOnShard("B", shard -> transaction.execute(status ->
                jdbc.update("insert into customers (id, name) values (?, ?)", "maintenance", "only on " + shard)));
        assertThat(databases.customers("B-primary")).containsExactly("maintenance");
        assertThat(databases.customers("A-primary")).isEmpty();

        // A transaction that fails leaves nothing behind
        assertThatThrownBy(() -> shards.executeOnShard("A", shard -> transaction.execute(status -> {
            jdbc.update("insert into customers (id, name) values (?, ?)", "half-done", shard);
            throw new IllegalStateException("the job broke");
        }))).hasMessageContaining("the job broke");
        assertThat(databases.customers("A-primary")).isEmpty();

        // The same on every shard: each callback has its own transaction on its own shard
        KurinShardOperations.ShardResults<Integer> results = shards.executeOnAllShards(shard -> transaction.execute(status -> {
            jdbc.update("insert into customers (id, name) values (?, ?)", "seed-" + shard, shard);
            return jdbc.queryForObject("select count(*) from customers", Integer.class);
        }));
        assertThat(results.allSucceeded()).isTrue();
        assertThat(results.get("A").value()).isEqualTo(1);
        assertThat(results.get("B").value()).isEqualTo(2);
        assertThat(databases.customers("A-primary")).containsExactly("seed-A");
        assertThat(databases.customers("B-primary")).containsExactly("maintenance", "seed-B");
    }
}
