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
