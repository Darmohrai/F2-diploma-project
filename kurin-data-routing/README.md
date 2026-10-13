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

`DataSourceHealthMonitor` checks every node with `Connection.isValid` on its own threads. A node is taken out
after a failed check or a connection failure reported by the router, and returns after a number of consecutive
good checks. Listeners get up and down transitions.

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
