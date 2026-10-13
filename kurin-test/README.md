# kurin-test

A Raft cluster inside one JVM, for tests of code built on Kurin: real nodes on free loopback ports, with a
transport that can cut, delay and drop traffic between chosen nodes.

```java
try (KurinTestCluster cluster = KurinTestCluster.builder()
        .nodes(3)
        .services(() -> List.of(new Accounts()))        // a fresh set of services per node
        .build()
        .start()) {

    KurinTestCluster.Node leader = cluster.awaitLeader(Duration.ofSeconds(10));
    leader.kurin().submitCommand(new Deposit("a", 10)).get(5, TimeUnit.SECONDS);
    cluster.awaitInSync(Duration.ofSeconds(5));          // every running node has applied everything

    cluster.partition(Set.of(leader), Set.copyOf(cluster.followers()));
    // ... the majority side elects a new leader, the old one steps down
    cluster.heal();

    cluster.kill(leader);                                // abrupt stop
    cluster.restart(leader);                             // same identity and data directory
}
```

| Call | Effect |
|---|---|
| `awaitLeader(timeout)` | Waits until one leader is acknowledged by a majority and known to every reachable node. |
| `awaitApplied(index, timeout)`, `awaitInSync(timeout)` | Wait for the running nodes to apply up to an index, or up to the leader's commit index. |
| `partition(groups...)` | Nodes of different groups cannot reach each other. Nodes in no group keep all their links. |
| `delay(from, to, d)`, `dropRate(from, to, rate)` | One-directional faults between two nodes. |
| `heal()` | Removes all faults. |
| `kill(node)`, `restart(node)` | Stops a node and starts it again; with `storage(FILE)` and a `dataDir` its state survives. |
| `node.service(Type.class)` | The service instance of that node, to inspect its state. |

`storage(...)` defaults to memory; use `RaftConfig.StorageType.FILE` with `dataDir(tempDir)` for restart tests.
`configure((index, builder) -> ...)` gives access to each node's builder.

Also here: `TestPorts.freePort()`, and `InMemorySharedExternalStore`, an `ExternalCacheStore` shared by several
cache managers in one JVM, standing in for an external cache.

Everything waits on conditions; nothing in this module sleeps for a fixed time.
