# Kurin

Kurin is a Java 21 framework for Spring applications that run as several instances. Each instance embeds a
node of a Raft cluster, and on top of that cluster the instances coordinate: run code on exactly one of them,
take cluster-wide locks, keep caches in step, and route database access.

No external coordinator (ZooKeeper, etcd, Redis) is needed: the instances are the cluster.

## Modules

| Module | What it is |
|--------|------------|
| [`core`](core/README.md) | Embedded Raft node on Netty and Kryo. Plain Java. |
| [`kurin-spring-boot-starter`](kurin-spring-boot-starter/README.md) | Auto-configuration of the node, `KurinTemplate`, metrics, tracing, Actuator. |
| [`kurin-locks`](kurin-locks/README.md), [`kurin-locks-spring-boot-starter`](kurin-locks-spring-boot-starter/README.md) | Fenced locks, leader-only code, run-once-per-slot jobs. |
| [`kurin-tiered-cache`](kurin-tiered-cache/README.md), [`kurin-cache-spring-boot-starter`](kurin-cache-spring-boot-starter/README.md) | Memory / disk / external cache with invalidation replicated to all instances. |
| [`kurin-data-routing`](kurin-data-routing/README.md), [`kurin-data-routing-spring-boot-starter`](kurin-data-routing-spring-boot-starter/README.md) | Read/write splitting with read-your-writes across instances, shard routing. Independent of the cluster. |
| [`kurin-test`](kurin-test/README.md) | In-process test cluster with fault injection. |

Plain modules have no Spring at runtime; Spring lives in the `*-spring-boot-starter` modules.

## Quick start

```xml
<dependency>
    <groupId>org.kurin</groupId>
    <artifactId>kurin-locks-spring-boot-starter</artifactId>
    <version>1.1.0</version>
</dependency>
```

```properties
# The same on every instance, except advertised-host
kurin.node.advertised-host=10.0.0.11
kurin.node.port=9400
kurin.node.peers=10.0.0.11:9400,10.0.0.12:9400,10.0.0.13:9400
```

```java
@Scheduled(cron = "0 */5 * * * *")
@KurinScheduledOnce(name = "sync-rates", slot = "PT5M")   // one instance per five-minute slot
public void syncRates() { ... }

@KurinDistributedLock(key = "'invoice:' + #id", onBusy = OnBusy.THROW)
public void settle(String id) { ... }

@Transactional
@KurinCacheEvict(cacheNames = "accounts", key = "#id")     // dropped on every instance after commit
public void rename(String id, String name) { ... }
```

## Guarantees, and what is not guaranteed

Read this section before relying on the framework for correctness.

### What goes through Raft and what is local

| Replicated through Raft (linearizable, survives failover) | Local to an instance |
|---|---|
| State of services with `@KurinCommand` handlers | Contents of the memory and disk cache tiers |
| Lock table, with fencing tokens | Which threads wait for a lock |
| Job claims and their history | Leader tasks and their schedule |
| Cache invalidations (`@KurinCacheEvict`, `KurinCacheInvalidator`) | Database routing decisions, node health |

A command is committed once a majority of nodes has it on disk. A cluster of `2f + 1` nodes survives `f` failures.
With fewer than a majority alive nothing can be committed: commands fail with `KurinNoLeaderException` or
`KurinTimeoutException`, and `KurinTemplate.leadership()` is empty everywhere.

### Commands

- A command is applied **at most once per request id**, on every node, in the same order.
- After `KurinTimeoutException` the outcome is **unknown**: the command may have been applied. Retry with the same
  `CommandOptions.requestId` to get the original result instead of a second application. The deduplication table
  holds the last `kurin.node.dedup-cache-size` request ids.
- An exception thrown by a handler is the result of the command (`KurinCommandFailedException`), identical on all
  nodes. It does not roll back what the handler changed before throwing.
- **Handlers must be deterministic.** Every node runs the handler and must reach the same state. A handler must not
  read the clock, generate random values, or call a database or another service. For time use
  `KurinCommandContext.current().timestampMs()`: the leader's clock when the command was logged, the same on every
  node. A handler that writes to a database writes N times, once per node; keep such writes outside of commands and
  use `@KurinCacheEvict` on the ordinary method instead.
- A service that keeps state must implement `@KurinSnapshot` / `@KurinRestore`; the node refuses to start with a
  service that handles commands and has neither these methods nor `@KurinService(stateless = true)`. Without them a node that catches up from a
  snapshot loses that state.

### Reads

| `ReadConsistency` | Sees | Cost | Fails when |
|---|---|---|---|
| `LINEARIZABLE` (default) | every command acknowledged before the read | a heartbeat round, unless the leader lease is valid | no leader |
| `LEASE` | the same, assuming clocks drift less than the lease margin | none | the leader holds no valid lease |
| `LOCAL` | whatever this node has applied; may be arbitrarily stale | none | never |

### Time

There are three clocks, and they are not interchangeable:

- **Log time** (`KurinCommandContext.timestampMs()`): wall clock of the leader at append. Used for lock and job
  expiry. Identical on all nodes; never runs backwards inside a state machine, but can stand still across a leader
  change if the new leader's clock is behind.
- **Monotonic local time** (`System.nanoTime()`): used for election timeouts and the leader lease. Leases assume
  that clocks of different nodes run at nearly the same *rate*; the lease is `election-timeout-min * lease-ratio`
  (default 80%), leaving the rest as margin.
- **Wall clock of an instance**: used only to compute the slot of `@KurinScheduledOnce` and cache entry lifetimes.

### Leadership

- `@KurinLeaderOnly` and `KurinLeaderTasks` start work only on a node that holds a valid leader lease. Two nodes
  never hold a valid lease at the same time, provided clock rates are sane.
- The check is made when the work **starts**. Work that runs for longer than the lease can overlap with the next
  leader. Pass `LeadershipToken.term()` to the systems you modify and let them reject older terms.

### Locks

- At most one owner per key is recorded at any point of the log, and each grant has a larger **fencing token** than
  all earlier grants.
- A lock is a **lease** of `ttl`. A holder that pauses longer than the ttl (GC, a stalled VM, a partition) may still
  be running when the lock is granted to someone else. `LockHandle.isHeld()` is conservative but cannot stop a
  thread. For resources that must never see two writers, check the fencing token at the resource.
- A lock request whose outcome is unknown is undone in the background; it does not linger until its ttl.

### Run-once jobs

- `AT_MOST_ONCE`: a slot is granted to one instance, once. If that instance dies mid-run, the slot is **not**
  repeated.
- `AT_LEAST_ONCE`: a slot whose owner does not report completion within `lease` can be taken over, so the job
  **may run twice** for that slot (the first owner may only be slow). The second run has a larger fencing token.
- Slots are computed from each instance's wall clock, rounded to the nearest slot boundary. **Clock skew between
  instances must stay well below half a slot**, otherwise two instances can see different slots for the same
  trigger and both run.

### Cache

- Entries live in the tiers of each instance. They are **not** replicated; only invalidations are.
- After a replicated invalidation completes on an instance, that instance serves no value loaded before the
  invalidation. Between the database commit and the invalidation reaching another instance, that instance can
  serve the old value: the window is one Raft commit, typically milliseconds. With the cluster down, an
  invalidation reaches only the calling instance and the shared tier; other instances serve their copies until the
  entry expires. Always set a `ttl`.
- Stampede protection `CLUSTER` gives one load for the whole cluster only if the cache has a shared external tier;
  otherwise it is one load at a time.

### Data routing

- A write inside a transaction always goes to the primary.
- `READ_YOUR_WRITES` holds across instances for writes made in transactions, for clients that return their
  consistency token. With the default `TIMESTAMP` strategy it is a bet that replicas lag less than
  `kurin.routing.sticky-window`; with `REPLICA_POSITION` it is exact.
- There are no cross-shard transactions. A second shard inside a transaction is refused.
- The framework plans resharding (`RingDiff`) but does not move data.

## Configuration reference

Durations accept ISO-8601 (`PT5S`) and simple forms (`5s`, `100ms`).

### `kurin.node.*`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `cluster-name` | String | `kurin` | Nodes with different names refuse to connect. |
| `advertised-host` | String | - | Address other nodes use to reach this one; part of the node id. Required unless all peers are loopback. |
| `bind-host` | String | `0.0.0.0` | Interface to listen on. |
| `port` | int | - | Port of the node protocol. Required. |
| `peers` | List | empty | Cluster members as `host:port`; may include this node. |
| `storage` | `FILE` \| `MEMORY` | `FILE` | Where term, vote, log and snapshots are kept. |
| `data-dir` | Path | `./kurin-data/<host>-<port>` | Directory of durable state. One per instance. |
| `heartbeat-interval` | Duration | `100ms` | Leader heartbeat period. |
| `election-timeout-min` | Duration | `1s` | Lower bound of the election timeout; at least 5 heartbeats. |
| `election-timeout-max` | Duration | `2s` | Upper bound of the election timeout. |
| `lease-ratio` | double | `0.8` | Share of `election-timeout-min` for which a leader trusts its lease. |
| `command-timeout` | Duration | `5s` | Default wait for a command or query. |
| `max-payload-size` | DataSize | `1MB` | Largest message on the wire. |
| `max-entries-per-append` | int | `512` | Entries per replication request. |
| `max-inflight-appends` | int | `1` | Replication requests in flight per follower. |
| `snapshot-threshold` | long | `10000` | Applied entries between snapshots. |
| `dedup-cache-size` | int | `10000` | Request ids remembered for deduplication. |
| `allow-services-without-snapshot` | boolean | `false` | Start even if a service with command handlers has no snapshot methods and is not declared stateless. Its state is then lost after log compaction. |
| `storage-retry-min` | Duration | `100ms` | Delay before the first attempt to use the storage again after an I/O failure; doubles with every failed attempt. |
| `storage-retry-max` | Duration | `10s` | Longest delay between such attempts. |
| `cluster-secret` | String | - | Enables HMAC authentication between nodes. |
| `idle-timeout` | Duration | `30s` | Idle node connections are closed after this. |
| `tls.enabled` | boolean | `false` | Mutual TLS between nodes. |
| `tls.key-store`, `tls.key-store-password`, `tls.key-store-type` | String | -, -, `PKCS12` | Certificate and key of this node. |
| `tls.trust-store`, `tls.trust-store-password`, `tls.trust-store-type` | String | -, -, `PKCS12` | Certificates of the other nodes or their CA. |
| `health.no-leader-threshold` | Duration | `10s` | Time without a leader after which health is `OUT_OF_SERVICE`. |

`kurin.serialization.types` (List of classes, default empty): types that travel inside commands or results but
cannot be discovered from handler signatures.

### `kurin.locks.*`, `kurin.jobs.*`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `kurin.locks.command-timeout` | Duration | `3s` | Wait for one lock command. |
| `kurin.locks.purge-interval` | Duration | `30s` | How often the leader removes expired lock records. |
| `kurin.jobs.history-size` | int | `50` | Slots remembered per job. |
| `kurin.jobs.command-timeout` | Duration | `3s` | Wait for a claim or a completion report. |

### `kurin.cache.*`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `data-dir` | Path | `<node data dir>/cache` | Disk tiers of this instance. |
| `defaults.ttl` | Duration | `10m` | Lifetime of a value; `0` means forever. |
| `defaults.null-ttl` | Duration | same as `ttl` | Lifetime of a cached null. |
| `defaults.jitter-percent` | int | `0` | Random spread of the lifetime, 0-100. |
| `defaults.stale-while-revalidate` | Duration | `0` | How long an expired value is served while it is refreshed. |
| `defaults.l1-max-size` | long | `10000` | Entries in the memory tier. |
| `defaults.tiers` | List of `L1`,`L2`,`L3` | all available | Tiers of a cache. |
| `caches.<name>.*` | | | The same keys per cache; unset ones fall back to `defaults`. |
| `codec.allowed-packages` | List | empty | Packages whose classes may be read from disk and external tiers. Required with a shared external store. |
| `l2.sweep-interval` | Duration | `1m` | Period of the sweep of expired disk entries. |
| `l2.sweep-batch-size` | int | `1000` | Entries examined per sweep and cache. |
| `l3.timeout` | Duration | `100ms` | Longest call into the external store. |
| `l3.failure-threshold` | int | `5` | Consecutive failures that open the circuit. |
| `l3.open-duration` | Duration | `10s` | Time the circuit stays open before a probe. |
| `l3.write-stripes` | int | `8` | Ordered write queues to the external store. |
| `invalidation.timeout` | Duration | `3s` | Wait for the cluster to replicate an invalidation. |
| `invalidation.epoch-cache-size` | long | `100000` | Recently invalidated keys remembered against stale write-backs. |
| `web.trace-header.enabled` | boolean | `false` | Adds the `X-Kurin-Cache` response header. |

### `kurin.routing.*`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `readonly-transactions-to-replica` | boolean | `false` | Let `@Transactional(readOnly = true)` use a replica. |
| `consistency-strategy` | `TIMESTAMP` \| `REPLICA_POSITION` \| `LOCAL_SESSION` | `TIMESTAMP` | How read-your-writes is decided. |
| `sticky-window` | Duration | `3s` | For `TIMESTAMP` / `LOCAL_SESSION`: how long a writer reads from the primary. |
| `probe-cache` | Duration | `200ms` | For `REPLICA_POSITION`: reuse of a probed replica position. |
| `postgres-probe.enabled` | boolean | `false` | Built-in PostgreSQL position probe. |
| `token-secret` | String | - | Signs consistency tokens; the same on all instances. |
| `token.header-name` | String | `X-Kurin-Consistency` | Header carrying the token; empty disables it. |
| `token.cookie-name` | String | empty | Cookie carrying the token; empty disables it. |
| `replica-fallback` | `PRIMARY` \| `FAIL` | `PRIMARY` | What a read does when its replica is down. |
| `virtual-nodes` | int | `160` | Ring points for a shard of weight 1. |
| `shards.<name>.weight` | int | from topology | Relative share of keys of the shard. |
| `health.enabled` | boolean | `true` | Periodic checks of database nodes. |
| `health.interval` | Duration | `5s` | Time between checks. |
| `health.timeout` | Duration | `1s` | Longest check. |
| `health.recovery-threshold` | int | `2` | Good checks before a node is used again. |
| `health.primary-required` | boolean | `false` | Report `OUT_OF_SERVICE` when a primary is down. |
| `fan-out.parallelism` | int | `8` | Shards worked on at once by `executeOnAllShards`. |

## Observability

With Micrometer on the classpath and a `MeterRegistry` bean, the starters publish `kurin.raft.*`, `kurin.locks.*`,
`kurin.jobs.*`, `kurin.cache.*` and `kurin.routing.*` meters (listed in the module READMEs). With an
`ObservationRegistry` they report observations `kurin.command`, `kurin.query`, `kurin.command.forward`,
`kurin.raft.apply`, `kurin.lock.acquire`, `kurin.cache.get`, `kurin.cache.load`; with a tracing bridge these become
spans, and the trace of a caller continues on the leader when a follower forwards its request. With Actuator there
are the health indicators `kurin`, `kurinCache`, `kurinRouting` and the endpoint `/actuator/kurin`. Log lines written
by the Raft thread carry the MDC keys `kurin.node`, `kurin.term`, `kurin.role`.

## Building

```
./mvnw clean install                 # all modules with tests
./mvnw -pl kurin-locks -am test      # one module and what it depends on
```

Upgrading from 1.0.x: see [MIGRATION-1.1.md](MIGRATION-1.1.md).
