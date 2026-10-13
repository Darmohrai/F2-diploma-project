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

- In a sharded topology, a connection requested with no shard selected fails with `KurinRoutingException`; there
  is no default shard.
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

## Events, health, metrics

`KurinPrimaryDownEvent`, `KurinPrimaryUpEvent`, `KurinReplicaDownEvent`, `KurinReplicaUpEvent` are published on
transitions.

Health `kurinRouting`: `UP` with `degraded: true` while any node is down; `OUT_OF_SERVICE` only when a primary is
down and `kurin.routing.health.primary-required=true`.

| Meter | Type | Tags |
|---|---|---|
| `kurin.routing.connections` | counter | `shard`, `role` |
| `kurin.routing.fallbacks` | counter | `shard`, `reason` |
| `kurin.routing.node.up` | gauge 0/1 | `shard`, `role` |
| `kurin.routing.replica.lag` | gauge: positions behind the primary; only with a `ReplicaPositionProbe` | `shard` |
| `kurin.routing.fanout.duration` | timer | |
| `kurin.routing.fanout.failed.shards` | counter | |

## Configuration

`kurin.routing.*`: see the [configuration reference](../README.md#configuration-reference).
