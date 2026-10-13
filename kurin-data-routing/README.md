# kurin-data-routing

Building blocks for choosing a database per call: primary or replica, and which shard. Plain Java, independent
of the Raft cluster.

## Topology

```java
KurinDataSourceTopology topology = KurinDataSourceTopology.builder()
        .shard("eu", euPrimary, euReplica)
        .shard("us", usPrimary, usReplica, 2)        // weight 2: twice the share of keys
        .build();

KurinDataSourceTopology single = KurinDataSourceTopology.of(primary, replica);   // no sharding
```

A replica may be null: reads of that shard go to its primary.

## Routing context

`KurinRoutingContext` holds the decision for the current thread as nested scopes:

```java
try (var read = KurinRoutingContext.readOnly(KurinReadOnly.Consistency.EVENTUAL);
     var shard = KurinRoutingContext.shard("eu")) {
    ...                                               // connections obtained here: replica of "eu"
}
executor.submit(KurinRoutingContext.propagating(task));   // carries the scopes to another thread
```

Scopes restore the previous decision when closed, so nested calls cannot leak a decision to their caller.

## Read-your-writes

A `ConsistencyToken` records, per shard, what the client last wrote. It travels with the client (header or
cookie), signed with HMAC when a secret is set, so the instance that serves the next read knows about a write
made through another instance.

| Strategy | Token holds | A read uses the replica when | Exact |
|---|---|---|---|
| `TimestampTokenStrategy` | time of the last write | the sticky window has passed | no: assumes lag below the window |
| `ReplicaPositionTokenStrategy` | replication position after the write | the replica has replayed up to that position (`ReplicaPositionProbe`) | yes |

`PostgresReplicaPositionProbe` reads WAL positions; other databases supply their own `ReplicaPositionProbe`.
A token that is malformed or fails its signature check is ignored: the client is treated as one that has not
written, so its reads may go to a replica. A client can therefore only weaken its own guarantee, never force
other clients' reads onto the primary with a forged token.

## Health

`DataSourceHealthMonitor` knows which nodes are usable. A node is taken out of use

- when the periodic check failed `failureThreshold` times in a row (2 by default), or
- at once, when the router reports that a request could not connect to it.

It is used again after `recoveryThreshold` good checks in a row. Listeners get the transitions.

```java
DataSourceHealthMonitor monitor = new DataSourceHealthMonitor(
        Duration.ofSeconds(5), Duration.ofSeconds(1),      // interval, longest check
        2, 2,                                              // failure threshold, recovery threshold
        new DriverNodeProbe());
monitor.register("eu", DataSourceType.REPLICA, replicaPool);
monitor.start();
```

**A pool without a free connection is not a node that is down.** Under load a pool runs out of connections and
callers time out waiting; the database behind it is in order, and marking it down would move its load to the
nodes that are left. `ConnectionFailureClassifier` tells the cases apart:

| `FailureKind` | Recognised by | Node |
|---|---|---|
| `CONNECTION_FAILED` | SQLState class `08`, `SQLNonTransientConnectionException`, a refused, reset or timed out socket, an unknown host; also a pool timeout whose cause is one of these (HikariCP reports its last failed attempt to create a connection as the cause) | out of use |
| `AUTHENTICATION_FAILED` | SQLState class `28`, `SQLInvalidAuthorizationSpecException` | out of use |
| `POOL_EXHAUSTED` | the timeout of HikariCP (`SQLTransientConnectionException` without state and cause), Tomcat JDBC (`PoolExhaustedException`) or Commons Pool behind DBCP (`Timeout waiting for idle object`), with nothing in the cause chain that points at the node | **stays as it is**; `Listener.onPoolExhausted` is called |
| `UNKNOWN` | anything else | out of use |

Whatever is not recognised counts against the node, so an unknown way of failing is never mistaken for load. The
price is that a pool this classifier does not know behaves as before 1.1.2.

**Checks do not go through the pool.** A `NodeProbe` makes the check:

- `DriverNodeProbe` (default) keeps one connection of its own per node, opened through the JDBC driver with the
  URL and credentials read from the data source (`getJdbcUrl()` or `getUrl()`, `getUsername()`, `getPassword()`;
  HikariCP, Tomcat JDBC, DBCP and Spring's simple data sources have them). It asks that connection `isValid` at
  every check and replaces it when it stopped working, so a restart of the database between two checks is not a
  failed check. The check is given up after the timeout whatever the driver does. One connection per node and
  application instance is the whole cost; a new connection per check would fail exactly when the database is at
  its connection limit.
- `PoolNodeProbe` borrows a connection from the pool for every check, as all versions before 1.1.2 did. A check
  that finds the pool exhausted waits for the pool's own timeout and learns nothing; it changes no state.
- A node whose data source does not tell its URL, or whose URL has no registered driver, is checked through the
  pool by `DriverNodeProbe` too, with a warning at registration. The same happens if the probe's own connection
  has never worked although the pool does (the pool adds properties the URL lacks, or gets its credentials
  elsewhere): the probe says so once and uses the pool for that node from then on.
- `new DriverNodeProbe((node, dataSource) -> Optional.of(new ConnectionParameters(url, user, password)))` supplies
  the parameters where the data source cannot; any other `NodeProbe` can check in a way of its own.

The longest time between a node going away and its being taken out by the checks alone is
`failureThreshold * (interval + timeout)`; when the node refuses connections outright, which is the usual case,
it is `failureThreshold * interval` plus one check. A request that hits the dead node takes it out sooner.
`KurinRoutingHealth` reports why a node is down (`failure`) and what the failure was taken for (`failureKind`).

## Shards

```java
ConsistentHashRing ring = new ConsistentHashRing(Map.of("eu", 1, "us", 2), 160);
String shard = ring.nodeFor("customer-42");

RingDiff diff = ring.diff(new ConsistentHashRing(Map.of("eu", 1, "us", 2, "ap", 1), 160));
diff.movedFraction();            // share of the key space that changes owner
diff.moves("customer-42");       // does this key move, and diff.oldOwner / diff.newOwner
```

- Placement is a pure function of the key and the shard names and weights: the same on every instance and after
  every restart. Keys are hashed by `String.valueOf(key)`, so use key types whose string form is stable (strings,
  numbers, UUIDs), not objects with the default `toString` or arrays.
- Adding a shard moves only the keys the new shard takes over.
- `ShardResolver` is the extension point for other placement rules (directories, ranges, pinned tenants).
- `KurinShardOperations` runs a callback on one shard, on the shard of a key, or on all shards in parallel with
  a timeout and a failure policy (`FAIL_FAST` or `COLLECT`), returning per-shard results.

## Not provided

- Moving data between shards. `RingDiff` tells which key ranges move where; copying them is the application's job.
- Transactions across shards.
- Failover of a primary. A dead primary fails writes with `KurinPrimaryUnavailableException`; promotion is done
  by the database tooling.
