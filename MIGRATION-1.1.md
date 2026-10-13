# Migrating from Kurin 1.0.x to 1.1.0

1.1.0 is not wire- or disk-compatible with 1.0.x: upgrade all nodes of a cluster together and start them with
empty data directories.

## Build

- All artifacts are version `1.1.0`.
- `core` no longer brings `slf4j-simple`. Applications choose their own SLF4J binding (Spring Boot already does).
- Kurin libraries no longer ship an `application.properties`.

## Data routing moved to its own modules

Routing and sharding were part of the cache modules. Depend on `kurin-data-routing-spring-boot-starter` instead of
relying on `kurin-cache-spring-boot-starter` for them.

| 1.0.x | 1.1.0 |
|-------|-------|
| `org.kurin.kurintieredcache.api.KurinReadOnly` | `org.kurin.routing.KurinReadOnly` |
| `org.kurin.kurintieredcache.api.KurinShardKey` | `org.kurin.routing.KurinShardKey` |
| `org.kurin.kurintieredcache.api.KurinRoutingContext` | `org.kurin.routing.KurinRoutingContext` |
| `org.kurin.kurintieredcache.api.DataSourceType` | `org.kurin.routing.DataSourceType` |
| `org.kurin.kurintieredcache.routing.ConsistentHashRing` | `org.kurin.routing.ConsistentHashRing` |
| `org.kurin.kurintieredcache.routing.KurinShardRouter` | `org.kurin.routing.KurinShardRouter` |
| `org.kurin.kurincachespringbootstarter.KurinRoutingDataSource` | `org.kurin.routing.spring.KurinRoutingDataSource` |
| `org.kurin.kurincachespringbootstarter.KurinDataSourceTopology` | `org.kurin.routing.spring.KurinDataSourceTopology` |
| `org.kurin.kurincachespringbootstarter.KurinClientIdResolver` | `org.kurin.routing.spring.KurinClientIdResolver` |
| `org.kurin.kurincachespringbootstarter.SessionStickyManager` | `org.kurin.routing.spring.SessionStickyManager` |
| `org.kurin.kurincachespringbootstarter.KurinRoutingAutoConfiguration` | `org.kurin.routing.spring.KurinRoutingAutoConfiguration` |

## Core API

### Node identity and configuration

- The node id is `advertisedHost:port`. `KurinNode.Builder.localNode(host, port)` sets the advertised host;
  `bindHost(...)` (default `0.0.0.0`) sets the listening interface.
- The peer list may contain the node itself; it is filtered out. Give every node the same list.
- `advertisedHost` is required as soon as a peer is not a loopback address.
- Durable state lives in `./kurin-data/<host>-<port>` unless `dataDir` is set. The old `raft-data-<port>` directory
  is not read.
- Election timing changed: heartbeat 100 ms, election timeout 1000-2000 ms (was 50 ms and 150-300 ms). Both are
  configurable; `electionTimeoutMin` must be at least five heartbeats.
- Nodes must share `clusterName` (default `kurin`) and, if set, `clusterSecret`.

### Commands and queries

- `KurinNode.Builder.registerCommand(Class)` is gone. Parameter and result types of `@KurinCommand` / `@KurinQuery`
  methods are registered automatically, including the types they contain. Use `registerType(Class)` only for types
  hidden behind `Object`, a non-sealed interface or an abstract class.
- Wire ids no longer depend on registration order. A type keeps its id as long as its fully qualified name does not
  change; annotate it with `@KurinType(id = N)` (N >= 1000) to make the id independent of the name.
- Nodes with different sets of registered types refuse to connect and log the reason. Deploy the same types to
  every node.
- `submitCommand` futures fail with subclasses of `org.kurin.api.exception.KurinException` instead of
  `RuntimeException("Network request failed")` or `IllegalStateException`:

  | Situation | Exception |
  |-----------|-----------|
  | The handler threw | `KurinCommandFailedException` (`causeType()`, `causeMessage()`) |
  | No leader is known | `KurinNoLeaderException` |
  | The contacted node is not the leader | `KurinNotLeaderException` (`leaderHint()`) |
  | No result in time, outcome unknown | `KurinTimeoutException` |
  | Type not registered, payload too large | `KurinSerializationException` |
  | Connection problems | `KurinRemoteException` |
  | Node not started or already stopped | `KurinNotStartedException` |

- `submitCommand` always times out (default 5 s). Use `submitCommand(cmd, Duration)` or
  `submitCommand(cmd, CommandOptions)`; pass a `requestId` in `CommandOptions` to retry idempotently.
- A `void` command submitted on a follower now completes normally (it used to time out).
- A handler exception no longer breaks the node: it is the result of the command, identical on every node.
- Handlers must be deterministic. Replace `System.currentTimeMillis()` inside `@KurinCommand` methods with
  `KurinCommandContext.current().timestampMs()`.

### Snapshots

- `@KurinSnapshot` methods must take no parameters and return `byte[]`; `@KurinRestore` methods must take a single
  `byte[]`. Other signatures fail at startup.
- The part of a snapshot that belongs to a service is keyed by `@KurinService("name")` or, without it, by the fully
  qualified class name.

### Listeners and status

- `KurinRoleChangeListener` is replaced by `org.kurin.raft.state.RoleChangeListener`, whose methods receive the
  `RaftStatus` at the moment of the change.
- `KurinNode.getRaftState()` is gone. Read `KurinNode.status()` (an immutable `RaftStatus`) instead.
- `StateMachineListener` and `RoleChangeListener` run on the Raft thread: they must not block.

### Queries and leadership

- `submitQuery(query)` is now linearizable (ReadIndex) instead of "whatever the leader has". Pass
  `ReadConsistency.LEASE` or `ReadConsistency.LOCAL` to trade freshness for latency.
- `KurinNode.isLeader()` only says that the node believes it is the leader. Use `KurinNode.leadership()` for
  decisions that must hold on a single node; its token is present only under a valid lease.
- A leader that loses contact with the majority steps down by itself after `electionTimeoutMin`.

### Lifecycle and persistence

- `KurinNode.Builder.build()` no longer requires services and no longer validates handlers. Both happen in
  `KurinNode.start()`. Services, listeners and types can be added to the built node until it is started.
- `KurinNode.serializer()` throws `KurinNotStartedException` before `start()`.
- With `storage = FILE` (the default) the log and snapshots are now really persisted, under
  `<dataDir>/log` and `<dataDir>/snapshot.bin`. A node restarts with its state instead of an empty log.
- The log is compacted automatically every `snapshotThresholdEntries` applied entries (default 10 000). A service
  that keeps state must implement `@KurinSnapshot` / `@KurinRestore`, otherwise that state is lost on nodes that
  catch up from a snapshot. A service declaring only one of the two fails at startup.

## Spring starter (`kurin.node.*`)

- `kurin.node.host` is renamed to `kurin.node.advertised-host`. It may be omitted only when all peers are loopback
  addresses.
- New properties: `cluster-name`, `bind-host`, `data-dir`, `storage`, `heartbeat-interval`, `election-timeout-min`,
  `election-timeout-max`, `lease-ratio`, `command-timeout`, `max-payload-size`, `max-entries-per-append`,
  `max-inflight-appends`, `snapshot-threshold`, `dedup-cache-size`, `cluster-secret`, `idle-timeout`, `tls.*`.
  Durations are `java.time.Duration` values such as `100ms` or `2s`.
- `kurin.serialization.types` and `KurinTypeRegistrar` beans register types that cannot be discovered from handler
  signatures.
- The node is started by a `SmartLifecycle` in phase 0, after all singletons exist and before the web server.
  Commands submitted earlier (for example from `@PostConstruct`) fail with `KurinNotStartedException`.
- A bean with Kurin handler methods can now inject `KurinTemplate` or `KurinNode`.
- State machine service beans must be eagerly initialized singletons. A lazy service created after the node
  started fails with an explicit error.
- Invalid handler signatures, conflicting type ids and invalid timing settings fail the application startup.
- `KurinNodeBuilderCustomizer.customize(builder, services)` is split: `KurinNodeBuilderCustomizer.customize(builder)`
  for configuration, `KurinNodeCustomizer.customize(node, services)` for work that needs the discovered services.
- `KurinLeaderElectedEvent` and `KurinLeaderLostEvent` have a new constructor and carry `term`, `leaderId` and
  `nodeId`. They are published on a separate thread, no longer on the Raft thread.
- `KurinTemplate.submitQuery` is renamed to `query`; `query(q, ReadConsistency)`, `submitCommand(cmd, Duration)`,
  `submitCommand(cmd, CommandOptions)`, `isLeader()`, `status()` and `leadership()` are new.
- There is no `KryoSerializer` bean any more, and `new KryoSerializer(List)` no longer exists. Use
  `KurinNode.serializer()` after start, or `KryoSerializer.standalone(types...)` outside of a node.

## Locks and coordination

### `kurin-locks`

- `DistributedLockService`, `AcquireLockCommand` and `ReleaseLockCommand` are replaced by `LockStateMachine` and
  the records in `LockCommands`. Lock state of a 1.0.x cluster cannot be carried over.
- `KurinLockManager` has a new constructor, `KurinLockManager(KurinNode, LockStateMachine, Duration commandTimeout)`,
  and a new API:

  | 1.0.x | 1.1.0 |
  |-------|-------|
  | `boolean tryLock(String key, long ttlMs)` | `Optional<LockHandle> tryLock(String key, Duration ttl)` |
  | `void unlock(String key)` | `LockHandle.release()` (or try-with-resources) |
  | - | `tryLock(key, ttl, waitTimeout)`, `lock(key, ttl, waitTimeout)`, `tryLock(key, LockOptions)` |

- A lock is no longer owned by "node plus thread id". Every acquisition has its own owner; only the `LockHandle`
  it returned can release or renew it. Acquiring the same key twice from one thread is refused unless
  `LockOptions.reentrant(true)` is used for both.
- Every grant carries a `fencingToken()`, and expiry is decided by the replicated log, identically on all nodes.
- `KurinLockAcquisitionException` now extends `KurinException`.

### Spring starter

- `@KurinDistributedLock`:
  - `ttlMs` (long) is replaced by `ttl` (duration string, default `PT30S`);
  - `key` is always a SpEL expression. A constant key must be quoted (`key = "'nightly'"`). An expression that
    fails or yields null now throws `KurinLockKeyException`; it used to be taken as a literal key;
  - new attributes `waitTimeout`, `onBusy` (default `SKIP`, the former behaviour), `autoRenew`, `reentrant`;
  - with `onBusy = SKIP` the method must not return a primitive: the application fails to start otherwise.
- `@KurinLeaderOnly` now requires established leadership with a valid lease, not just the leader role, and has
  `onNotLeader = SKIP | THROW`. With `SKIP` a primitive return type fails the startup.
- New: `@KurinLeaderTask`, `@KurinScheduledOnce`, `KurinLeaderTasks`, `KurinJobs`, `KurinLockContext`,
  `KurinLeadership`, `KurinJobContext`.
- `KurinLockTemplate.executeWithLock(key, ttlMs, action)` takes a `Duration` now.
- Properties: `kurin.locks.lock-timeout-ms` and `kurin.locks.unlock-timeout-ms` are replaced by
  `kurin.locks.command-timeout`. New: `kurin.locks.purge-interval`, `kurin.jobs.history-size`,
  `kurin.jobs.command-timeout`.

## Cache

The cache was rewritten. Data in 1.0.x disk tiers is not read; delete the old cache directories.

### Annotations

- `@KurinCacheable`:

  | 1.0.x | 1.1.0 |
  |-------|-------|
  | `tier = CacheTier.TIERED_SMART` | `tiers = {}` (default: the configured tiers) or `tiers = {CacheTier.L1, CacheTier.L2}` |
  | `preventStampede = true`, `stampedeLockTtlMs` | `stampede = StampedeMode.CLUSTER`, `lockWait = "PT5S"`, `onLockTimeout` |
  | `jitter = 20` | `jitterPercent = 20` |
  | - | `ttl`, `nullTtl`, `condition`, `unless`, `staleWhileRevalidate` |

  `stampede` defaults to `LOCAL`: concurrent misses of one key on one instance now share a single load.
- `key` is always SpEL. A constant key must be quoted (`key = "'all'"`). An expression that fails or yields null
  throws `KurinCacheKeyException`; it used to become one literal key shared by all calls.
- The default key changed from `method:hash(args)` to `Class.method(arg1,arg2,...)`. Arguments must be strings,
  numbers, booleans, enums, UUIDs, `java.time` values, records, collections, maps or arrays of those; for other
  types give an explicit `key`.
- `@KurinCacheEvict(cacheName = "x")` becomes `@KurinCacheEvict(cacheNames = "x")`. It may now be put on ordinary
  methods: it then evicts after the method (after commit, inside a transaction) on all instances.
- `@KurinCachePut` and `@KurinCacheEvict` need a `key` expression unless the first parameter implements
  `CacheKeyProvider` (or `allEntries` is set). `command.toString()` is no longer used as a key.
- Misused annotations (unparsable expression or duration, `cacheNulls` on a primitive, a void `@KurinCacheable`)
  fail the application startup.

### Configuration

- Without configuration a value now lives 10 minutes (`kurin.cache.defaults.ttl`). Before, values without `jitter`
  never expired in L2 and lived 15 minutes after last access in L1.
- `kurin.cache.storage-dir` becomes `kurin.cache.data-dir` and defaults to `<node data dir>/cache`.
- `kurin.cache.l1-max-size`, `l1-expire-minutes` become `kurin.cache.defaults.l1-max-size` and
  `kurin.cache.defaults.ttl`; both can be set per cache under `kurin.cache.caches.<name>`.
- `kurin.cache.l3-max-failures`, `l3-circuit-open-timeout-ms` become `kurin.cache.l3.failure-threshold` and
  `kurin.cache.l3.open-duration`; `kurin.cache.l3.timeout` (default 100 ms) is new.
- With a shared external store, `kurin.cache.codec.allowed-packages` must list the packages of the cached classes.

### SPI

- `CacheProvider`, `CacheConfigurationContext`, `DefaultKurinCacheManager`, `TieredCompositeCache`,
  `FaultTolerantCacheDecorator`, `RaftCacheUpdater` and `KurinCacheProxy` are removed.
- An external tier is no longer a `CacheProvider` bean for `L3_EXTERNAL` but an `ExternalCacheStore` bean that
  stores bytes: `get`, `put(key, bytes, ttl)`, `delete`, `deleteByPrefix`, `isShared`. It needs no serializer.
- `KurinCacheManager` is a class built with `KurinCacheManager.builder()`; `getOrCreateCache(name, tier)` becomes
  `getCache(name)`, which returns a `TieredCache`.
- `KurinCache.get` returns a `CacheLookup` (entry plus tier) instead of the raw value, `put` takes a `CacheEntry`,
  and `putWithJitter` is gone: the lifetime comes from `CacheSettings.newEntry`.
- `CacheEntry` has four components (`payload`, `createdAtMs`, `expiresAtMs`, `staleUntilMs`).
- `CacheTier` values are `L1`, `L2`, `L3`.
- New: `KurinCacheInvalidator`, `KurinCacheObserver`, `KurinCacheTrace`, `KurinCacheValueCodec`, `KeyGenerator`,
  `CacheClusterLock`.

## Data routing

(Package moves are listed at the top of this document. The changes below are relative to the routing code that
existed before 1.1, which was never part of a release.)

- The `DataSource` to inject is now the `@Primary` bean `kurinDataSource`, a lazy proxy around
  `KurinRoutingDataSource`. Code that injected `KurinRoutingDataSource` to run queries should inject `DataSource`.
- `KurinDataSourceTopology`, `ConsistentHashRing` and `KurinShardRouter` moved to `org.kurin.routing` and
  `org.kurin.routing.shard`. `KurinShardRouter` is built from the shard names and a `ShardResolver`.
- `KurinRoutingContext.set/get/clear` are replaced by scopes: `try (var s = KurinRoutingContext.readOnly(...))`,
  `primary()`, `shard(name)`.
- `@KurinReadOnly` gained `consistency` (default `READ_YOUR_WRITES`). A write transaction inside a
  `@KurinReadOnly` method now goes to the primary instead of the replica.
- A connection requested without `@KurinReadOnly` no longer marks the client as a writer. The mark is set when a
  read-write transaction commits.
- `SessionStickyManager` is gone. Its behaviour is `kurin.routing.consistency-strategy=local-session`; the default
  is `timestamp`, which works across instances through the `X-Kurin-Consistency` token.
- `kurin.routing.sticky-window-ms` becomes `kurin.routing.sticky-window` (a duration).
- New: `@KurinPrimary`, `@KurinSharded`, `KurinShardOperations`, `ShardResolver`, `RingDiff`, node health with
  `KurinPrimaryUnavailableException` and the node state events, `KurinCrossShardTransactionException`.

## Observability

Nothing here breaks existing code unless it implements or constructs the types below.

- `ClientCommandRequest` and `ClientQueryRequest` gained a `headers` component (trace context forwarded to the
  leader). Code that constructs these records directly passes an empty map; all nodes of a cluster must run the
  same version, as for every wire change in 1.1.
- New in `core`: `KurinObserver` (callbacks for elections, commits, applies, RPCs, snapshots) and
  `KurinInstrumentation` (spans). Register them with `KurinNode.Builder.addObserver(...)` and
  `instrumentation(...)`. Both are called on the Raft thread and must not block.
- New on `KurinNode`: `startSpan`, `logEntries`, `leaderUnknownFor`, `peerContactAges`.
- `KurinCacheAspect` has a new constructor `(support, reader, invalidator, node)`. Applications that declared
  the aspect themselves instead of using the auto-configuration pass the node.
- With `micrometer-core` on the classpath and a `MeterRegistry` bean, the starters now register meters named
  `kurin.raft.*`, `kurin.locks.*`, `kurin.jobs.*`, `kurin.cache.*`, `kurin.routing.*`. With an
  `ObservationRegistry` bean they report observations (`kurin.command`, `kurin.query`, `kurin.command.forward`,
  `kurin.raft.apply`, `kurin.lock.acquire`, `kurin.cache.get`, `kurin.cache.load`).
- With `spring-boot-actuator` on the classpath there are new health contributors `kurin`, `kurinCache`,
  `kurinRouting`, and an endpoint `kurin`. An application whose readiness probe includes all health contributors
  will now report `OUT_OF_SERVICE` when the node has had no leader for longer than
  `kurin.node.health.no-leader-threshold` (default 10 s). To keep the old behaviour, exclude the contributor
  from the probe group, or declare your own bean named `kurinHealthIndicator`, which replaces the built-in one.
- Log lines written on the Raft thread carry the MDC keys `kurin.node`, `kurin.term`, `kurin.role`.

## Documentation

`README.md` in the root and in every module now states what is and is not guaranteed. Two claims of the 1.0
documentation were wrong and are removed: serialization is not zero-copy (messages are encoded by Kryo into
buffers and copied), and election timeouts are randomized, not deterministic.

---

# 1.1.0 → 1.1.1

## Services without snapshot methods no longer start

A bean or service with `@KurinCommand` handlers and no `@KurinSnapshot` / `@KurinRestore` methods was accepted
with a warning. It now fails the start:

```
Service com.example.Counters handles commands but has no @KurinSnapshot/@KurinRestore methods. ...
```

Pick one:

- the service keeps state: add the two methods (`byte[] snapshot()` and `void restore(byte[])`);
- its commands keep nothing in the service (they only return a result, or act through cache annotations and
  state machine listeners): declare it `@KurinService(stateless = true)`;
- you accept the loss for now: `kurin.node.allow-services-without-snapshot=true`
  (`KurinNode.Builder.allowServicesWithoutSnapshot(true)`), which logs a warning per service.

`@KurinService.value` now has a default (empty: the class name). `@KurinService("name")` works as before.

## Storage failures

- New exception `KurinStorageException` (a `KurinException`). A node that cannot write its log, snapshot or
  term/vote file refuses commands and queries with it until the storage works again. Code that distinguishes
  Kurin exceptions should treat it like `KurinNotLeaderException`: the request can be retried, on another node
  or later, with the same request id.
- `RaftStatus` has a new component `storageFailure` (null while storage works) and `storageFailed()`. The
  eight-argument constructor is kept.
- `KurinObserver` has two new default methods, `onStorageFailure` and `onStorageRecovered`.
- `RaftLog` has a new default method `recover()`, `SnapshotStore` a new default method `verifyWritable()`. A custom
  implementation backed by files should implement them; one that reports I/O failures should throw
  `UncheckedIOException`. `FilePersistentMetadata.save` now throws `UncheckedIOException` instead of
  `IllegalStateException`.
- The health contributor `kurin` reports `DOWN` while the storage of the node is failed. A liveness probe that
  includes it will restart the instance; if the disk is expected to come back, keep it in the readiness group only.
- New properties `kurin.node.storage-retry-min` (100 ms) and `kurin.node.storage-retry-max` (10 s).

## Health indicators moved to adapter modules

The starters no longer contain health indicators. Add the adapter for your Spring Boot generation:

```xml
<dependency>
    <groupId>org.kurin</groupId>
    <artifactId>kurin-actuator-boot3</artifactId>   <!-- Spring Boot 3.2 - 3.5 -->
    <version>1.1.1</version>
</dependency>
<!-- or kurin-actuator-boot4 for Spring Boot 4.x -->
```

Without it the application works as before, with metrics and `/actuator/kurin`, but `/actuator/health` has no
`kurin`, `kurinCache` and `kurinRouting` entries.

- `org.kurin.kurinspringbootstarter.observability.KurinHealthIndicator` is removed. The verdict is
  `org.kurin.raft.KurinNodeHealth.of(node, noLeaderThreshold)`; the bean `kurinHealthIndicator` is now a
  `HealthIndicator` declared by the adapter.
- The indicators can be switched off the standard way: `management.health.kurin.enabled=false`,
  `management.health.kurin-cache.enabled=false`, `management.health.kurin-routing.enabled=false`. A bean with the
  name of an indicator still replaces it.
- With the routing starter and an adapter, the `db` health of Spring Boot is replaced by a composite of the
  physical pools (`<shard>.primary`, `<shard>.replica`) plus any data source that is not behind the routing. The
  old `db` entry checked the routing data source itself and was always `DOWN` with more than one shard.
  `management.health.db.enabled=false` switches it off; a bean named `dbHealthContributor` or `dbHealthIndicator`
  replaces it.
- `/actuator/kurin` has a new section `routing` when both the node starter and the routing starter are used.

## Spring Boot 4

Kurin 1.1.1 runs on Spring Boot 3.2 - 3.5 and on Spring Boot 4.x with the same jars; see
`docs/boot-compatibility.md`. Nothing changes for an application on Spring Boot 3 except the adapter above.

- The starters no longer bring `spring-boot-starter-aop` (it does not exist under that name in Spring Boot 4) but
  `spring-aop` and `aspectjweaver`. An application that relied on the starter being there transitively, for
  aspects of its own, should declare it.

## Data routing with several shards

- **Access without a shard is allowed while the application starts.** Until now it failed at any time, which made
  an application with JPA, Liquibase or Flyway and more than one shard fail to start. The new default,
  `kurin.routing.unrouted.mode=startup-only`, sends such access to the primary of the first shard (or
  `kurin.routing.unrouted.shard`) until the application is ready, and fails afterwards as before. For the old
  behaviour set `kurin.routing.unrouted.mode=fail`.
- **Liquibase and Flyway migrations run on the primary of every shard.** If the application already migrated its
  shards by other means and wants to keep doing so, set `kurin.routing.migrations.enabled=false`. Change sets
  that were applied by hand without Liquibase or Flyway knowing will now be attempted; mark them as run first.
- `KurinRoutingDataSource` has a second constructor with the unrouted mode, the unrouted shard and a supplier
  telling whether the application is ready. The old constructor is kept and behaves like the mode `fail`.
- New beans: `KurinRoutingStartup`, `KurinRoutingDiagnostics`.
- Tests that build a context with `ApplicationContextRunner` never get an `ApplicationReadyEvent`: such a context
  stays in its startup phase. Set `kurin.routing.unrouted.mode=fail`, or call `KurinRoutingStartup.finish()`, where
  a test relies on access without a shard being refused.
- New observation `kurin.routing.fanout` with a child `kurin.routing.fanout.shard` per shard, when an
  `ObservationRegistry` bean exists. `KurinShardOperations` has a new hook, `setInstrumentation`.

---

# 1.1.1 → 1.1.2

1.1.2 fixes four defects. No public signature was removed or changed. Two fixes change what callers observe;
both bring the behaviour in line with what the documentation of 1.1.0 and 1.1.1 already promised.

## Locks: a cluster that cannot answer is no longer reported as "busy"

`KurinLockManager.tryLock` returned `Optional.empty()`, and `lock` and `@KurinDistributedLock(onBusy = THROW)`
threw `KurinLockAcquisitionException`, both when another owner held the lock and when the cluster gave no answer
at all (no leader, a node out of reach, a timeout). The READMEs said that the two are never confused. Now they
are not:

| Outcome | `tryLock` | `lock`, `KurinLockTemplate.executeWithLock` | `@KurinDistributedLock` |
|---|---|---|---|
| another owner holds the lock | empty | `KurinLockAcquisitionException` | as `onBusy` says |
| the cluster gave no definite answer | throws | throws | throws, whatever `onBusy` says |

What is thrown in the second row is `KurinNoLeaderException`, `KurinRemoteException` or
`KurinTimeoutException`, with the original failure as the cause. All are `KurinException`s; none is a
`KurinLockAcquisitionException`.

What to check in your code:

- **`tryLock(...)` without a try/catch**, where "empty" was read as "someone else is doing it": it can now throw.
  If skipping is what you want in both cases, catch `KurinException`; more often the two deserve different
  handling, which is the point of the fix.
- **`@KurinDistributedLock` with `onBusy = SKIP`** on a method triggered by `@Scheduled` or a message listener:
  when the cluster is unavailable the method used to be skipped silently and now throws into the scheduler or
  the listener container, which log it. The method body still does not run.
- **`catch (KurinLockAcquisitionException e)`** now handles "busy" only. A cluster failure passes it by.
- **With a `waitTimeout`**, attempts that got no answer are repeated until the wait is over, as before; if the
  last one still got none, its exception is thrown instead of `KurinLockAcquisitionException`.

`@KurinScheduledOnce` and `KurinJobs.runOnce` keep returning without running the job when the claim of a slot
gets no answer. The difference is that this is now logged at WARN and counted separately
(`kurin.jobs.claims{result="unknown"}`) instead of being counted as `denied`.

New, additive API: `KurinLockManager.LockListener.onAcquireFailed` / `onReleaseUnconfirmed` and
`KurinJobs.JobListener.onClaimUnknown` (default methods), constructors with a cause on `KurinNoLeaderException`
and `KurinTimeoutException`, meters `kurin.locks.acquire{result="unknown"}` and
`kurin.locks.release.unconfirmed`.

## Transport: forwarded requests wait for the connection to the leader

Nothing to change in applications. What is observable:

- A command or query submitted on a follower no longer fails at once with
  `KurinRemoteException: Node ... is unreachable, next connection attempt is delayed` while the node pauses
  between connection attempts. It waits for the next attempt, within its own timeout. If that attempt fails, the
  exception is `KurinRemoteException: Cannot connect to ...` with the cause; if the timeout ends first, it is
  `KurinTimeoutException`. Code that retried on the old message can drop the retry.
- A node that started before its peers connects to a peer as soon as it hears from it.
- The `kurin` health has a new detail, `commandPath` (`UP`, `CONNECTING`, `DOWN`), also available as
  `KurinNode.commandPath()`. The status is computed as before. `CONNECTING` on a follower that forwards nothing is
  normal.

## `@KurinLeaderTask`: `interval` takes property placeholders

Additive. `interval` of `@KurinLeaderTask`, and with it `ttl` and `waitTimeout` of `@KurinDistributedLock` and
`slot` and `lease` of `@KurinScheduledOnce`, are resolved against the environment when the context starts:
`"${outbox.relay.interval}"`, `"${outbox.relay.interval:2s}"`. A number without a unit is milliseconds, as for
`@Scheduled(fixedDelayString)`; `PT2S` and `2s` work as before.

A value that used to be rejected at startup as an unparsable duration is still rejected. What is new is that a
`${...}` is no longer such a value. A placeholder without a value and without a default stops the startup with
the bean, the method and the attribute in the message.

If you construct `KurinLocksAspect` yourself, the three-argument constructor still exists and takes the
attributes as written; pass the `KurinCoordinationRegistry` bean as a fourth argument to have placeholders
resolved.

## Data routing: a busy pool is no longer a dead node

What callers observe differently:

- **A request that times out waiting for a connection pool fails with the pool's exception.** Before, a read on
  an exhausted replica pool marked the replica down and was served by the primary, and a write on an exhausted
  primary pool got `KurinPrimaryUnavailableException`. Now the node stays up, the read is not moved, and the
  caller gets what the pool threw (wrapped by Spring as usual, for example in `CannotGetJdbcConnectionException`).
  If your code relied on reads spilling over to the primary under load, it relied on the defect; size the replica
  pool for its load, or route such reads to the primary explicitly with `@KurinPrimary`.
- **One failed health check no longer takes a node out.** `kurin.routing.health.failure-threshold` is new and
  defaults to `2`: a node goes out after two failed checks in a row, which is one `interval` later than before
  (5 s with the defaults). A request that cannot connect to the node still takes it out at once. Set the
  property to `1` for the old timing.
- **Health checks open a connection of their own.** `kurin.routing.health.probe` is new and defaults to `driver`:
  each application instance keeps one extra connection per database node, opened through the JDBC driver with
  the URL, user and password read from the node's data source. Count it against `max_connections`. The check no
  longer waits for the application's pool. Set `kurin.routing.health.probe=pool` for the old check; a pool
  timeout during such a check is then ignored instead of taking the node down.
- A node whose data source does not expose a JDBC URL is checked through the pool whatever the setting, and a
  warning says so at startup. Declare a `NodeProbe` bean to check such a node differently.
- The health details of a node that is down have a new entry, `failureKind`. New meters:
  `kurin.routing.pool.exhausted{shard,role}`, `kurin.routing.node.transitions{shard,role,to}`.

API, all additive: `ConnectionFailureClassifier`, `NodeProbe`, `DriverNodeProbe`, `PoolNodeProbe`,
`ConnectionParameters`; `DataSourceHealthMonitor` has a five-argument constructor, `reportConnectionFailure`,
`failureKindOf` and `Listener.onPoolExhausted` (a default method); `KurinRoutingHealth.NodeHealth` has a sixth
component `failureKind`, and its five-argument constructor remains. The three-argument constructor of
`DataSourceHealthMonitor` now uses a failure threshold of 2 and a `DriverNodeProbe`; for the behaviour of 1.1.1
use `new DataSourceHealthMonitor(interval, timeout, 1, recoveryThreshold, new PoolNodeProbe())`.
