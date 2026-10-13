# kurin-data-routing-spring-boot-starter

Read/write splitting and shard routing for JDBC, on top of
[kurin-data-routing](../kurin-data-routing/README.md). It does not need a Kurin node and can be used alone.

Declare the topology; the starter publishes a `@Primary DataSource` that routes each connection:

```java
@Bean
KurinDataSourceTopology topology() {
    return KurinDataSourceTopology.builder()
            .shard("eu", euPrimary(), euReplica())
            .shard("us", usPrimary(), usReplica())
            .build();
}
```

The pools themselves must not be `DataSource` beans, or must not be `@Primary`: the routing data source is the
one JPA, `JdbcTemplate` and the transaction manager should see.

## Primary or replica

```java
@KurinReadOnly                                          // replica, unless this client wrote recently
public List<Order> history(String customerId) { ... }

@KurinReadOnly(consistency = Consistency.EVENTUAL)     // always replica; may miss the caller's own write
public Stats dashboard() { ... }

@KurinPrimary                                           // the latest committed data, whoever wrote it
public Order forPayment(String orderId) { ... }

@Transactional
public void place(Order order) { ... }                  // read-write transaction: primary
```

First match wins:

1. `@KurinPrimary`: primary.
2. A read-write transaction is active: primary, even inside `@KurinReadOnly`. A write can never land on a replica.
3. `@KurinReadOnly` (or `@Transactional(readOnly = true)` with `kurin.routing.readonly-transactions-to-replica`):
   replica, unless read-your-writes sends this client to the primary.
4. Otherwise: primary.

Then health: a read whose replica is down goes to the primary (`replica-fallback=PRIMARY`) or fails (`FAIL`);
anything that needs a dead primary fails at once with `KurinPrimaryUnavailableException`.

A pool that has no free connection is not a dead node. A request that times out waiting for a pool fails with the
exception of the pool, as on any saturated pool: the read is not moved to the primary, the node is not marked
down, and a write does not get `KurinPrimaryUnavailableException`. See [Node health](#node-health).

The annotations work on methods and classes, are inherited from interfaces, and take effect before the
transaction starts. Calls inside one object bypass them, as with any Spring AOP annotation.

## Read-your-writes across instances

When a read-write transaction commits, the client's consistency token is updated and returned in the
`X-Kurin-Consistency` response header (and a cookie, if configured). The client sends it back; whichever instance
serves the next request knows about the write.

| `kurin.routing.consistency-strategy` | Works across instances | Exact | Needs |
|---|---|---|---|
| `TIMESTAMP` (default) | yes | no: assumes replicas lag less than `sticky-window` | the client to return the token |
| `REPLICA_POSITION` | yes | yes | a `ReplicaPositionProbe` bean, or `postgres-probe.enabled=true` |
| `LOCAL_SESSION` | no: one instance, or sticky load balancing | no | a `KurinClientIdResolver` (default: servlet principal or session id) |

Set `kurin.routing.token-secret` (the same on all instances) so tokens are signed. Outside of HTTP, carry the
token yourself: `KurinConsistency.current()` after a write, `KurinConsistency.withToken(token, action)` before a
read. Writes made outside a transaction (auto-commit) are not tracked: use transactions for writes.

## Shards

```java
public Order find(@KurinShardKey String customerId, String orderId) { ... }

public void save(@KurinShardKey Order order) { ... }      // Order has a field annotated with @KurinShardKey

@KurinSharded(key = "#request.customer.id")
public void place(PlaceOrder request) { ... }
```

- In a sharded topology, a connection requested with no shard selected fails with `KurinRoutingException` once
  the application is ready; what happens before, and how to name a default shard, is described
  [below](#database-access-before-a-shard-can-be-chosen).
- A transaction is pinned to the first shard it touches; a second shard raises
  `KurinCrossShardTransactionException`. There are no cross-shard transactions.
- Placement is weighted consistent hashing; replace it with a `ShardResolver` bean.
- Work on several shards with `KurinShardOperations`:

```java
ShardResults<Long> counts = shardOperations.executeOnAllShards(
        shard -> jdbc.queryForObject("select count(*) from orders", Long.class),
        FanOutOptions.defaults().readOnly(Consistency.EVENTUAL).timeout(Duration.ofSeconds(5))
                .failurePolicy(FailurePolicy.COLLECT));
long total = counts.values().stream().mapToLong(Long::longValue).sum();
counts.failures().forEach((shard, error) -> log.warn("Shard {} failed", shard, error));
```

Each shard's callback runs in its own thread and its own transaction scope.

## Database access before a shard can be chosen

With several shards, a connection requested with no shard selected has nowhere to go. But a good deal of what
happens at startup asks for one before any code of the application runs: Hibernate reads the JDBC metadata,
schema validation looks at the tables, a migration tool creates them, health checks probe every `DataSource`.
`kurin.routing.unrouted.mode` says what such access gets:

| Mode | During startup | After the application is ready |
|---|---|---|
| `startup-only` (default) | the **primary** of the unrouted shard | `KurinRoutingException` |
| `fail` | `KurinRoutingException` | `KurinRoutingException` |
| `default-shard` | the unrouted shard, primary or replica by the usual rules | the same |

- The unrouted shard is `kurin.routing.unrouted.shard`, by default the first shard of the topology.
- "Ready" is the `ApplicationReadyEvent` of the application's own context: after the context is refreshed and
  the application runners have run. The state is kept per application context. A context that is not started by
  `SpringApplication` never becomes ready by itself; call `KurinRoutingStartup.finish()` there.
- The first unrouted access during startup is logged at INFO with an explanation, the following ones at DEBUG.
- `default-shard` is for applications that keep global tables on one shard. It logs a warning at startup: a
  forgotten shard key is no longer detected, the statement just runs on that shard.
- With a single shard none of this applies; there is nothing to choose.

### Migrations on every shard

With several shards and Liquibase or Flyway on the classpath, the migrations Spring Boot configures from
`spring.liquibase.*` or `spring.flyway.*` run on the **primary of every shard**, one shard after another in the
order of the topology, instead of once. Replicas are not touched; they get the schema by replication. A failure
on a shard fails the start and names the shard.

The migration bean of the application is used as it is: for each shard it is pointed at the primary and run, so
every setting applies to every shard. A migration bean with a data source of its own (`spring.liquibase.url`,
`@LiquibaseDataSource`, `spring.flyway.url`) is left alone.

| Property | Default | Meaning |
|---|---|---|
| `kurin.routing.migrations.enabled` | `true` | `false` leaves the migration to Spring Boot, which then reaches the unrouted shard only. |
| `kurin.routing.migrations.shards` | all | Shards to migrate, in this order. |

With Flyway only `migrate()` is spread over the shards. For `info`, `validate`, `repair` or `clean` take the
`Flyway` bean, which is a `KurinShardedFlyway`, and call `forShard(name)`.

### Hibernate

In the default mode nothing is changed: Hibernate reads its metadata from the primary of the unrouted shard, and
`spring.jpa.hibernate.ddl-auto=validate` validates against it.

In the mode `fail` with several shards Hibernate cannot open that connection. Unless the application set
`hibernate.boot.allow_jdbc_metadata_access` (or `hibernate.temp.use_jdbc_metadata_defaults`) itself, the
starter switches the metadata access off, and the dialect has to be named with `spring.jpa.database-platform`.
If it is not, the start fails with a message that says so.

### Maintenance code

Code of the application that works on the databases outside of a request (data migrations, clean-up, reports)
runs through `KurinShardOperations`. Inside the callback the shard is selected, so `JdbcTemplate`, JPA and
transactions work as usual:

```java
// one shard
shardOperations.executeOnShard("eu", shard -> transactionTemplate.execute(status ->
        jdbc.update("delete from sessions where expires_at < ?", cutoff)));

// every shard, each callback in its own transaction on its own shard
ShardResults<Integer> deleted = shardOperations.executeOnAllShards(shard -> transactionTemplate.execute(status ->
        jdbc.update("delete from sessions where expires_at < ?", cutoff)));
```

## Node health

Every node (the primary and the replica of every shard) is checked every `kurin.routing.health.interval`.

| Property | Default | Meaning |
|---|---|---|
| `kurin.routing.health.enabled` | `true` | Run the periodic checks. Without them only failed requests take a node out, and nothing brings it back. |
| `kurin.routing.health.interval` | `5s` | Time between checks. |
| `kurin.routing.health.timeout` | `1s` | Longest time one check may take. |
| `kurin.routing.health.failure-threshold` | `2` | Failed checks in a row that take a node out. One failed check does nothing. |
| `kurin.routing.health.recovery-threshold` | `2` | Good checks in a row that bring it back. |
| `kurin.routing.health.probe` | `driver` | `driver`: the check has a connection of its own to each node, opened through the JDBC driver with the URL and credentials of the node's data source; it never waits for the pool of the application. `pool`: every check borrows a connection from that pool. |
| `kurin.routing.health.primary-required` | `false` | Report `OUT_OF_SERVICE` when a primary is down. |

What takes a node out, and what does not:

- A request that cannot connect (connection refused, unknown host, SQLState class `08`, rejected credentials, a
  pool that timed out because it could not create connections) takes the node out **at once**.
- `failure-threshold` failed checks in a row take it out. With the defaults a node that refuses connections is
  out within about 10 s without any request hitting it; in general within `failure-threshold * (interval +
  timeout)`.
- **A pool without a free connection takes nothing out.** The timeout of HikariCP, Tomcat JDBC or DBCP waiting
  for a free connection is counted in `kurin.routing.pool.exhausted` and logged at DEBUG. If that counter grows,
  the pool is too small for the load or connections are held too long; the database is not the problem.

With `probe=driver` each instance holds one extra connection per node. The parameters are read from the data
source (`getJdbcUrl()` / `getUrl()`, `getUsername()`, `getPassword()`); HikariCP, Tomcat JDBC and DBCP expose
them. For a data source that does not, the node is checked through the pool and a warning is logged at startup:

```
Database node eu/REPLICA is checked through the connection pool of the application: its data source (...)
does not tell how to connect to it. Such a check competes with the application for connections ...
```

The same fallback applies, with its own warning, when the connection of the check has never worked although
the pool does, for example because the pool sets properties that are not in the URL. To check differently,
declare a bean; it replaces the built-in probes:

```java
@Bean
NodeProbe nodeProbe() {
    return new DriverNodeProbe((node, dataSource) ->
            Optional.of(new ConnectionParameters(urlOf(node), "health", healthPassword)));
}
```

## Events, health, metrics

`KurinPrimaryDownEvent`, `KurinPrimaryUpEvent`, `KurinReplicaDownEvent`, `KurinReplicaUpEvent` are published on
transitions.

Health, with [kurin-actuator-boot3](../kurin-actuator-boot3/README.md) or
[kurin-actuator-boot4](../kurin-actuator-boot4/README.md): `kurinRouting` is `UP` with `degraded: true` while any node
is down, and `OUT_OF_SERVICE` only when a primary is down and `kurin.routing.health.primary-required=true`; `db`
lists the physical pools instead of the routing data source. A node that is down carries `failure`, the message of
what took it down, and `failureKind`, what that was taken for: `CONNECTION_FAILED`, `AUTHENTICATION_FAILED` or
`UNKNOWN`. The bean `KurinRoutingDiagnostics` gives the same facts to code.

| Meter | Type | Tags |
|---|---|---|
| `kurin.routing.connections` | counter | `shard`, `role` |
| `kurin.routing.fallbacks` | counter | `shard`, `reason` |
| `kurin.routing.node.up` | gauge 0/1 | `shard`, `role` |
| `kurin.routing.node.transitions` | counter: changes of a node between usable and not | `shard`, `role`, `to` = `up`, `down` |
| `kurin.routing.pool.exhausted` | counter: requests and checks that got no connection from a pool in time; the node stays up | `shard`, `role` |
| `kurin.routing.replica.lag` | gauge: positions behind the primary; only with a `ReplicaPositionProbe` | `shard` |
| `kurin.routing.fanout.duration` | timer | |
| `kurin.routing.fanout.failed.shards` | counter | |

With an `ObservationRegistry`: observation `kurin.routing.fanout` (tags `shards`, `failed`) for a call of
`executeOnAllShards`, in the trace of the caller, and a child `kurin.routing.fanout.shard` (tag `shard`) for the work
on each shard, current on the thread that does it.

## Configuration

`kurin.routing.*`: see the [configuration reference](../README.md#configuration-reference).
