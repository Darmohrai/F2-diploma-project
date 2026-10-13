# Kurin 1.1.0 — progress log

Work follows `KURIN_1.1_CLAUDE_CODE_PROMPT.md` phase by phase. Each phase ends with a green
`./mvnw clean install` and a commit on branch `feature/kurin-1.1`.

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Build and modules, type registry, single-threaded Raft state, timers, step-down, response envelope, apply semantics, identity, network | done |
| 2 | NOOP entry, check-quorum, lease, PreVote, read consistency, leadership API, fault injection tests | done |
| 3 | Snapshots, compaction, file persistence | done |
| 4 | Spring integration of the core | done |
| 5 | Locks, leadership tasks, exactly-once jobs | done |
| 6 | Cache | done |
| 7 | Data routing | done |
| 8 | Observability, documentation | done |

## Baseline

Commit `6020003` holds the work that was uncommitted on `master` when 1.1 started (cache stampede/null/jitter
options, read/write routing, shard router). Phase 1 builds on it.

## Phase 1

### Done

- **Build (1.1).** Version `1.1.0` everywhere. The root POM imports `spring-boot-dependencies` 3.2.12 and manages
  Kryo, RocksDB and the Kurin modules; module POMs carry no versions. `slf4j-simple` is test-scoped. `core` no
  longer depends on Spring at all. `HELP.md` files and library `application.properties` are removed. Awaitility and
  AssertJ are available in every module. `.gitignore` covers `raft-data-*/` and `kurin-data/`. Surefire sets
  `kurin.debug.thread-checks=true`.
- **Modules (1.2, 1.3).** Routing moved out of the cache modules into `kurin-data-routing` (`org.kurin.routing`) and
  `kurin-data-routing-spring-boot-starter` (`org.kurin.routing.spring`). `kurin-test` is a published module with
  `KurinTestCluster`.
- **Type registry (4).** `KurinTypeRegistry` assigns ids from `@KurinType` or a hash of the class name, registers
  types transitively (handler parameters and results, record components, fields, arrays, generic arguments, enums,
  permitted subclasses of sealed types), has fixed ids for JDK value types and collections, produces a fingerprint
  and can be frozen. One `KryoSerializer` per node backed by a Kryo `Pool`.
- **Single-threaded Raft state (2.1).** Inbound RPCs, timers, client submissions and response callbacks all run on
  the `kurin-raft-<port>` thread. `AsyncThreadPoolDispatcher` and `Mailbox` are gone. `RaftState.assertOnRaftThread()`
  guards the mutators. Other threads read the immutable `RaftStatus`, republished after every task.
- **Timers (2.2).** Wheel tick is 10 ms. Heartbeat and election timeouts are configurable and validated. Every timer
  start has an epoch, and a callback is ignored unless both its epoch and its role instance are still current.
- **Step-down (2.3).** `RaftState.stepDown(term, leaderHint)` is the only way to adopt a higher term; it clears the
  vote and persists before anything is answered. Term and vote are always saved together through
  `PersistentMetadata.save`.
- **Response envelope and errors (2.7).** Every request is answered with an `RpcResult`. The exception hierarchy
  lives in `org.kurin.api.exception`. `submitCommand` always has a timeout and accepts `CommandOptions`. A follower
  retries once at the hinted leader; `KurinNode` re-routes once when the local node loses leadership mid-request.
- **Apply semantics (2.8).** A handler exception is a `CommandOutcome` failure: `lastApplied` advances, the client
  gets `KurinCommandFailedException`, every node records the same result. Listener exceptions are isolated.
  `LogEntry` carries `type` and `leaderTimestampMs`; `KurinCommandContext` exposes them to handlers. A resubmitted
  request id joins the entry already in the log; the table of processed requests evicts in insertion order so that
  all nodes forget the same ids.
- **Identity (2.12).** Node id is `advertisedHost:port`; `bindHost` is separate and defaults to `0.0.0.0`. Peers are
  filtered of the local node. A loopback identity with non-local peers logs a warning. The leader is forgotten on a
  term change, and a candidate answers `KurinNoLeaderException`.
- **Network (3).** Handshake (`Hello`, hand-encoded) checks cluster name, protocol version, registry fingerprint and
  an optional HMAC. Connections are removed on close with `remove(key, value)`, re-established with exponential
  backoff and jitter (50 ms to 2 s), and a write to a channel that died is retried once. Requests are bound to their
  channel and fail at once when it closes. The encoder rejects oversized payloads without closing the connection;
  the decoder reports an undecodable payload to the peer instead of closing. Optional mutual TLS. Idle timeout is
  configurable. `PING`, `sendOneWay` and the dead code around them are removed.
- **Tests.** Spec tests 1, 5, 6, 9, 10 in `kurin-test` (`RaftClusterTest`, `HandshakeTest`), plus
  `KurinTypeRegistryTest` in `core`.

### Decisions and deviations

- **Replication flow control (2.9) was implemented in phase 1**, not phase 2. `LeaderRole` was rewritten for the
  single-threaded model anyway, and writing it once with per-peer in-flight limits, batch limits and
  `conflictIndex`/`conflictTerm` was safer than first writing a version that floods followers.
- **Snapshot RPCs were removed for now.** The 1.0.4 `InstallSnapshot` path was incorrect (see 2.10) and nothing
  compacts the log before phase 3. Rather than keep a known-wrong implementation, `InstallSnapshotRequest/Response`,
  `FileRaftLog` and `triggerSnapshot` were deleted and come back in phase 3. Wire ids 27 and 28 stay reserved.
- **`FILE` storage persists only term and vote until phase 3.** The log is in memory, as in 1.0.4. `dataDir` and
  `storage` already exist so that tests never write to the working directory.
- **`FilePersistentMetadata` was fixed now** (atomic move with fallback, fatal on failure) although the spec lists it
  under 2.11, because the old `File.renameTo` silently stopped persisting votes on Windows.
- **Tests of the Raft core live in `kurin-test`.** `core` cannot depend on `kurin-test` (it would be a cycle), so the
  cluster-level tests are the test suite of the `kurin-test` module itself.
- **`RpcResult` keeps the five fields of the spec.** For `COMMAND_FAILED` the class name of the handler exception
  travels in `value`, so that `KurinCommandFailedException.causeType()` can be rebuilt on the caller.
- **Unknown JDK classes are not auto-registered** during the transitive walk. Kryo would fall back to reflective
  field access, which the module system forbids. They can be registered explicitly with `registerType`.
- **`KurinCacheSpringBootStarterApplicationTests` was deleted.** It was a generated `contextLoads` stub without a
  node port, so it could never pass.
- **Spring starters and the cache/locks modules are only adapted to compile** against the new core API in this
  phase. Their rewrites, including removal of fixed ports and `Thread.sleep` from their tests, belong to phases 4-7.

## Phase 2

### Done

- **NOOP entry (2.4).** A new leader appends a `NOOP` entry of its term and replicates it. Until that entry
  commits the leader holds no lease, serves no linearizable or lease reads and exposes no leadership token.
  `NOOP` entries never reach the state machine.
- **Check-quorum (2.5).** For every peer the leader remembers when the latest answered request was *sent*. On each
  heartbeat tick it steps down if no majority answered within `electionTimeoutMin`.
- **Lease (2.5).** `leaseValidUntil` is the send time of the heartbeat round acknowledged by a majority plus
  `electionTimeoutMin * leaseRatio`, in `System.nanoTime()`. It is part of `RaftStatus`.
- **Leader stickiness (2.5).** A follower that heard from a leader within `electionTimeoutMin` refuses votes and
  pre-votes without adopting the candidate term. A leader with a valid lease refuses votes as well.
- **PreVote (2.5).** A follower whose election timer fires first asks the peers whether they would vote for it
  (`PreVoteRequest`, no term change) and becomes a candidate only on a majority of grants. A timed-out candidate
  falls back to follower and starts with a pre-vote again.
- **Read consistency (2.6).** `submitQuery(query, ReadConsistency)`. `LINEARIZABLE` uses ReadIndex: valid lease, or a
  heartbeat round sent after the read arrived and acknowledged by a majority. `LEASE` answers only under a valid
  lease. `LOCAL` runs on the receiving node. Followers forward the first two to the leader.
- **Leadership API (2.13).** `KurinNode.leadership()` returns a `LeadershipToken(term, nodeId, acquiredAtLogIndex)`
  only while the lease is valid and the NOOP of the term is committed.
- **Fault injection (10.1).** `KurinTestCluster.partition/heal/delay/dropRate` through `FaultInjectingTransport`.
- **Tests.** Spec tests 2, 3, 4 in `RaftSafetyTest`, plus tests of lease/local reads and of the leadership token.

### Decisions and deviations

- **PreVote runs inside `FollowerRole`** instead of a separate role. The node keeps answering RPCs as a follower
  while it probes, and an incoming heartbeat simply cancels the round.
- **Test 2 allows 250 ms of scheduling slack** on top of `electionTimeoutMin + heartbeat` when it measures how fast
  the isolated leader steps down. The bound in the spec is exact for the protocol; the test observes it through
  thread scheduling and a 5 ms polling loop.
- **`awaitLeader` waits for every running node the leader can reach**, not just a majority. Otherwise a test that
  immediately talks to the slowest follower can get `KurinNoLeaderException`, which is correct behaviour of the
  node but makes tests flaky. This was the one failure in 20 consecutive runs before the change.

## Phase 3

### Done

- **Automatic snapshots (2.10).** After applying, when `lastApplied - baseIndex >= snapshotThresholdEntries`, the Raft
  thread takes the state machine snapshot, saves it together with the deduplication table and compacts the log.
- **InstallSnapshot (2.10).** The leader sends the *stored* snapshot with its own `lastIncludedIndex/Term`, in
  chunks of `maxAppendBytes`, one chunk in flight per follower, serialized once per snapshot. The follower ignores
  snapshots at or below its commit index, reassembles the chunks, keeps the log suffix when its term matches and
  resets the log otherwise, and restores the deduplication table.
- **`PojoStateMachine` (2.10).** Snapshot parts are keyed by `@KurinService` or the class name. A service must
  declare both `@KurinSnapshot` and `@KurinRestore` or neither; a service with commands and no snapshot methods
  logs a warning at startup.
- **`FileRaftLog` (2.11).** Segmented files of `[length][crc32][kryo]` records, read back in one buffered pass. A
  torn or corrupt tail is cut off with a warning. Compaction atomically rewrites `log.meta` and deletes whole
  segments; `truncateFrom` atomically replaces the boundary segment.
- **Group commit (2.11).** Appends are buffered. The follower flushes once per `AppendEntries` batch before it
  acknowledges. The leader flushes when its task queue runs empty (at most every 64 tasks) and counts itself towards
  the quorum only up to `getDurableIndex()`.
- **`FileSnapshotStore` (2.11).** Checksummed, written to a temporary file and moved atomically.
- **Recovery (2.11).** On start the node loads the snapshot, reconciles the log base with it and leaves later
  entries unapplied until a leader confirms the commit index. Inconsistent or corrupt durable state fails the start.
- **Tests.** Spec tests 7 and 8 in `RaftPersistenceTest`; `FileRaftLogTest` and `DurableStateTest` in `core`.

### Decisions and deviations

- **Phases 2, 3 and 4 share one commit.** Each next phase was written while the 20-run stability check of the
  previous one was still running, and they touch the same classes (`LeaderRole`, `FollowerRole`, `KurinNode`). The
  work was verified per phase: 20 consecutive green runs of the `kurin-test` suite after phase 2 (one failure before
  the `awaitLeader` change described above) and after phase 3, and a full `clean install` after phase 4.
- **All live log entries stay in memory** next to the files. The log is bounded by the snapshot threshold, and this
  keeps reads on the Raft thread free of disk access.
- **A leader may compact entries a slow follower still needs.** That follower then receives the snapshot. No
  retention margin was added; the snapshot threshold is the tuning knob.

## Phase 4

### Done

- **Deferred node start (5).** `KurinNode` is built without services and started later. `KurinServiceRegistry`, a
  static bean post-processor, collects beans with Kurin handler methods (including inherited ones) as they are
  created. `KurinNodeLifecycle` (`SmartLifecycle`, phase 0) freezes the registry, registers services and types,
  runs `KurinNodeCustomizer` beans and starts the node. Any validation error fails the context start.
- **No dependency cycle.** Creating the `KurinNode` bean needs no application bean, so services can inject
  `KurinNode` or `KurinTemplate`.
- **`KurinNotStartedException`** for commands submitted before the lifecycle started the node.
- **Events.** `KurinLeaderElectedEvent` / `KurinLeaderLostEvent` carry `term`, `leaderId`, `nodeId` and are published
  on the `kurin-events` thread.
- **Properties.** All of `kurin.node.*` from the spec, with `Duration` and `DataSize` types and generated IDE
  metadata; `kurin.serialization.types`; the `KurinTypeRegistrar` SPI.
- **`KurinTemplate`** with the command, query, status and leadership methods of the spec.
- **Tests.** Spec tests 31 and 32 in `KurinStarterIntegrationTest` and `KurinStartupValidationTest`.

### Decisions and deviations

- **`KurinNode` itself supports "create now, start later"** (services, listeners and types may be added until
  `start()`). The spec asks for this ordering in the Spring lifecycle; it cannot be done without the core allowing it.
- **Handlers are invoked on the unwrapped bean**, not through its Spring proxy. They run on the Raft thread during
  `apply`, where transactional or security advice would be wrong, and this also removes the problem of calling a
  class method on a JDK interface proxy.
- **Two customizer SPIs.** `KurinNodeBuilderCustomizer(builder)` adjusts configuration before the node object
  exists; `KurinNodeCustomizer(node, services)` runs right before start, when the services are known. The 1.0.x
  `KurinNodeBuilderCustomizer(builder, services)` could not keep its signature, because the builder is consumed
  before the services are discovered.
- **The `KryoSerializer` bean is gone.** The node serializer exists only after start. The cache uses a standalone
  serializer until its own codec arrives in phase 6.
- **`kurin.node.host` is renamed to `kurin.node.advertised-host`.**
- **`maven-compiler-plugin` runs with `<proc>full</proc>`.** JDK 23 stopped running classpath annotation processors
  by default, which silently dropped the configuration metadata.
- **State machine services must be eager singletons.** A service bean created after the node started cannot be
  added to the state machine; this fails with an explicit error instead of a silent "No handler found".

## Phase 5

### Done

- **Lock state machine (6.1).** `LockStateMachine` keeps `LockRecord(ownerId, fencingToken, expiresAtMs,
  acquiredAtMs)` per key. Its only clock is `KurinCommandContext.timestampMs()`, made monotonic inside the state
  machine. The fencing token is the log index of the granting command. Commands: `AcquireLock`, `RenewLock`,
  `ReleaseLock`, `PurgeExpiredLocks`; an acquire over an expired record overwrites it. Snapshot and restore.
- **`KurinLockManager` (6.1).** `tryLock(key, ttl)`, `tryLock(key, ttl, waitTimeout)`, `lock(...)`, `LockOptions`
  with `autoRenew` and `reentrant`, and `LockHandle` (`key`, `fencingToken`, `expiresAt`, `renew`, `release`,
  `isHeld`, `AutoCloseable`). The owner id is `nodeId:UUID` per attempt. An attempt without a definite answer is
  followed by a background `ReleaseLock` of that owner id, retried with backoff.
- **Waiting without polling (6.1).** Every node raises a local event when a key becomes free (release, purge,
  overwrite of an expired record). A parked thread is woken by it; the timed wait with exponential backoff and
  jitter (50-1000 ms) is only a safety net.
- **Leadership (6.2).** `@KurinLeaderOnly(onNotLeader = SKIP | THROW)` checks `leadership()`, binds the token to
  `KurinLeadership.currentToken()`. `KurinLeaderTasks` runs registered tasks only while the node holds established
  leadership and cancels running ones (signal plus interrupt) when it is lost. `@KurinLeaderTask` registers bean
  methods. The lock purge is such a task (`kurin.locks.purge-interval`).
- **Run once per slot (6.3).** `JobStateMachine` with `ClaimJob` / `CompleteJob`, `AT_MOST_ONCE` and
  `AT_LEAST_ONCE`, bounded history, `JobHistory` query. `KurinJobs.runOnce` and `@KurinScheduledOnce`; the slot is
  the wall clock rounded to the nearest slot boundary. `KurinJobContext.current()` exposes slot and fencing token.
- **`@KurinDistributedLock` (6.4).** `key` (SpEL, parsed once per method, failure or null is an exception), `ttl`,
  `waitTimeout`, `onBusy = SKIP | THROW | WAIT`, `autoRenew`, `reentrant`; `KurinLockContext.current()`;
  `KurinLockTemplate` on the new API.
- **Startup validation.** `SKIP` on a primitive return type, `WAIT` without a wait timeout, unparsable key
  expressions or durations, unsupported `@KurinLeaderTask` signatures and duplicate task names stop the application
  at startup.
- **Tests.** Spec tests 11-14 in `LockClusterTest`, 15 in `JobAndLeaderClusterTest` (programmatic) and
  `CoordinationClusterTest` (`@KurinScheduledOnce` on three application contexts), 16 in `CoordinationClusterTest`.

### Decisions and deviations

- **One waiter per node is woken per release**, the longest waiting first. Waking all of them would cost one Raft
  command per waiter for a lock only one can get, which is exactly what the spec wants to avoid.
- **A slot is a number, and the history size travels in `ClaimJob`.** Slots older than the retained history are
  refused for good (`prunedThroughSlot`), so pruning cannot make an `AT_MOST_ONCE` slot claimable again. Carrying
  the history size in the command keeps pruning identical on nodes with different local configuration.
- **A reported failure completes a slot.** `AT_LEAST_ONCE` re-runs a slot only when its owner went silent past the
  lease, as the spec words it, not when the job itself threw.
- **`onBusy = WAIT` requires a positive `waitTimeout`** and throws when it runs out. An unbounded wait on a
  cluster-wide lock is never what a request thread wants.
- **`isHeld()` is conservative.** The local deadline is measured from before the acquire or renew request was
  sent, so the handle never claims the lock longer than the cluster grants it.
- **`status()` is republished before a command future completes.** A caller that saw its command finish and then
  read `status().lastApplied()` could observe the previous value; two test failures in a 20-run loop traced back to
  this. `KurinTestCluster.awaitInSync` was added for tests that need all nodes caught up.
- **`KurinTestCluster.awaitLeader` now also requires a majority to follow the leader.** A leader that was just cut
  off keeps a valid lease for a moment and was returned as "the leader" by a test.

## Phase 6

### Done

- **Entry model and TTL in every tier (7.1).** One `CacheEntry(payload, createdAtMs, expiresAtMs, staleUntilMs)`
  travels through all tiers. L1 uses a per-entry Caffeine `Expiry`; L2 keeps the expiry in the encoded header,
  checks it on read and sweeps expired entries in bounded batches; L3 receives the remaining lifetime as its TTL.
  `kurin.cache.defaults.*` and `kurin.cache.caches.<name>.*` with `ttl`, `null-ttl`, `jitter-percent`,
  `stale-while-revalidate`, `l1-max-size`, `tiers`; annotation attributes win. Jitter is uniform over
  `ttl ± ttl * percent / 100`. The hard-coded one hour TTL is gone.
- **SPI (7.2).** `KurinCache` with `CacheLookup get`, `put(key, CacheEntry)`, `evict`, a real `clear()`, `close()`.
  `ExternalCacheStore` works on bytes under `kurin:<cache>:<key>` and declares `isShared()`. The disk tier clears
  with `deleteRange`, lives under the node data directory and uses LZ4.
- **Codec (7.3).** `KurinCacheValueCodec` SPI and `KryoCacheCodec`: its own Kryo without registration, header with
  magic, version, flags, `createdAt`, `expiresAt`, `staleUntil`. With `kurin.cache.codec.allowed-packages` it
  refuses every class outside those packages and a fixed set of JDK types, on write and on read; a shared external
  store without that list fails at startup. Hibernate proxies and lazy collections are refused with a clear message.
- **Keys (7.4).** SpEL keys with `#name`, `#a0`/`#p0`, `#cmd`, `#root.methodName`, `#root.target`, `#result`; a
  failing or null expression throws `KurinCacheKeyException`; parsed expressions are cached. The default key is
  `Class.method(args)` rendered by `CacheKeyRenderer`, collision-free for the supported types; other types are
  refused. `KeyGenerator` SPI.
- **Annotations (7.5).** `@KurinCacheable` (`tiers`, `ttl`, `nullTtl`, `jitterPercent`, `cacheNulls`, `condition`,
  `unless`, `staleWhileRevalidate`, `stampede`, `lockWait`, `onLockTimeout`), `@KurinCachePut`, `@KurinCacheEvict`
  (`cacheNames[]`, `key`, `allEntries`, `replicate`). On command handlers put and evict run on every instance at
  apply; on ordinary methods evict runs after the method, or after commit of a surrounding transaction.
- **Replicated invalidation (7.6).** `CacheInvalidationStateMachine` with `EvictCacheCommand`; the origin cleans
  the shared tier, then replicates; every instance drops its local tiers on apply. `KurinCacheInvalidator` returns
  a future completed when the invalidation is applied on the calling instance. `InvalidationEpochs` keeps a load
  that started before an invalidation from writing its result.
- **Writes per tier (7.7).** L1 and L2 synchronously; L3 through a striped executor that preserves the order of
  operations per key. A hit in a lower tier is copied up unchanged.
- **Stampede protection (7.8).** `LOCAL` single flight, `CLUSTER` single flight plus cluster lock with a re-check
  that includes the shared tier, `onLockTimeout = LOAD | FAIL | STALE`, stale-while-revalidate with one background
  refresh whose failure keeps the stale value.
- **L3 resilience (7.9).** Per-call timeout, circuit breaker with `CLOSED / OPEN / HALF_OPEN` and a single probe,
  failures never reach the caller.
- **Visibility (7.10).** `KurinCacheObserver`, `KurinCacheTrace`, optional `X-Kurin-Cache` servlet filter.
  Micrometer metrics follow in phase 8.
- **Removed (7.11).** `KurinCacheProxy`, the old `CacheProvider`, `TieredCompositeCache`, `FaultTolerantCacheDecorator`.
- **Tests.** Spec tests 18, 20, 22 and the local parts of 19 and 21 in `kurin-tiered-cache`; 17, the cluster part of
  21, 23, 24 and the external part of 19 in the cache starter (`CacheClusterTest`, `ExternalTierTest`).

### Decisions and deviations

- **`CacheProvider` was dropped instead of rewritten.** With L1 and L2 built in and L3 behind `ExternalCacheStore`
  there is nothing left for a provider to supply; a second extension point for the same thing would only confuse.
- **`CacheTier` is now `L1`, `L2`, `L3`.** `TIERED_SMART` is expressed as a list of tiers.
- **An instance also deletes from the shared tier what it recently wrote there itself.** The spec lets only the
  origin clean the shared tier, before the command. That leaves a hole: an instance that read the old value from the
  database just before the write can store it in the shared tier after the origin cleaned it, and nobody removes it
  again. Each instance therefore remembers for a minute which keys it wrote to the shared tier and deletes those
  when it applies an invalidation for them. The common case stays at one delete.
- **Invalidations missed through a snapshot clear the local tiers.** The invalidation state machine replicates
  only a counter. Each instance stores next to its disk tier how many invalidations it applied; if a snapshot
  brings a different number, the instance cannot know which keys it missed and drops its local cache data. With an
  in-memory Raft log the disk tier is cleared at start for the same reason.
- **Tests that need the shared store live in the cache starter module.** `InMemorySharedExternalStore` is in
  `kurin-test` as the spec asks, so `kurin-test` depends (optionally) on `kurin-tiered-cache`, and the plain cache
  module cannot depend on `kurin-test` in turn.
- **Command handlers evict the shared tier on every instance.** Each instance applies the command and none is "the
  origin"; repeated deletes are harmless.
- **`@KurinCacheEvict` on an ordinary method does not wait for replication.** The calling instance and the shared
  tier are clean when the method returns; a replication failure is logged.
- **`onLockTimeout = STALE` throws when there is no stale value**, like `FAIL`.
- **The default stampede mode is `LOCAL`.** It costs nothing and removes the most common stampede; `NONE` and
  `CLUSTER` are opt-in.
- **`cacheNulls` on a primitive return type and `@KurinCacheable` on a void method fail the startup.**
- **Stability.** 20 consecutive green runs of the cache suites (36 tests).

## Phase 7

### Done

- **Read/write split (8.1).** `@KurinReadOnly(consistency = EVENTUAL | READ_YOUR_WRITES)` and `@KurinPrimary`, on
  methods or classes. Inside an active read-write transaction every connection goes to the primary, also under
  `@KurinReadOnly`. `kurin.routing.readonly-transactions-to-replica` lets `@Transactional(readOnly = true)` use a
  replica. The "this client wrote" mark is set in `afterCommit` of a writing transaction.
- **Read-your-writes across instances (8.2).** `ConsistencyTokenStrategy` SPI with `TimestampTokenStrategy`
  (default), `ReplicaPositionTokenStrategy` on a `ReplicaPositionProbe` (cached, with a built-in
  `PostgresReplicaPositionProbe`) and `LocalSessionStrategy`. The token is stateless, HMAC-signed with
  `kurin.routing.token-secret`, and carried by `ConsistencyTokenCarrier`; the servlet filter uses the
  `X-Kurin-Consistency` header and optionally an `HttpOnly`, `SameSite=Lax` cookie. A token that fails verification
  is ignored with a warning. `KurinConsistency.withToken` serves non-web callers.
- **Node health (8.3).** `DataSourceHealthMonitor`: periodic `Connection.isValid`, immediate DOWN on a failed
  connection attempt, UP after N good checks. A dead replica sends reads to the primary (`replica-fallback`), a
  dead primary fails writes and strict reads with `KurinPrimaryUnavailableException` while eventual reads continue.
  `KurinReplicaDownEvent`, `KurinPrimaryDownEvent` and their `Up` counterparts.
- **Sharding (8.4).** `@KurinShardKey` on a parameter or field, `@KurinSharded(key = SpEL)`. `ShardResolver` SPI with
  the weighted `ConsistentHashShardResolver` (`kurin.routing.shards.<name>.weight`). `ConsistentHashRing.diff`
  returns a `RingDiff` with the moved ranges, `moves(key)` and the moved fraction. `KurinShardOperations` with
  `executeOnAllShards` (bounded pool, per-task context, `readOnly`, `timeout`, `FAIL_FAST | COLLECT`),
  `executeOnShard`, `executeForKey`. A second shard inside a transaction raises
  `KurinCrossShardTransactionException`.
- **Tests.** Spec tests 28 and 30 in `kurin-data-routing` (`ShardingTest`); 25, 26, 27, 29 in the starter
  (`RoutingIntegrationTest`), against H2 databases, one per node, each wrapped so that it can be switched off.

### Decisions and deviations

- **The application sees a `LazyConnectionDataSourceProxy` around the routing data source.** Spring publishes
  whether a transaction is read-only only after it has asked for the connection. Without the lazy proxy the router
  would have to decide before it can know, which is the very bug 8.1 describes.
- **`ConsistencyTokenStrategy.onWriteCommitted(shard, primary)` takes the primary `DataSource`**, which the spec
  signature omits. The position strategy has to query the primary there.
- **A write outside of a transaction is not noted in the token.** In auto-commit mode the router cannot tell a
  write from a read. Such access goes to the primary (there is no `@KurinReadOnly`), but read-your-writes is
  guaranteed only for writes made in transactions.
- **Without `kurin.routing.token-secret` tokens are unsigned**, with a warning at startup. A client can then only
  change where its own reads go.
- **Tests use H2 in-memory databases without replication.** Each node is a separate database with a marker row, so
  a query shows which node answered and a write shows where it landed. Docker is not assumed.
- **The routing starter does not depend on the Kurin node.** Routing needs no cluster; an application can use it
  alone.

## Phase 8

### Done

**Observability (spec section 9)**

- `core` got two dependency-free hooks. `KurinObserver`: elections, leader changes, commit latency, apply
  duration per command type, RPC latency per type and peer, append batch sizes, snapshots. `KurinInstrumentation`:
  spans with tags and propagation headers. Both are invoked on the Raft thread; a failing observer is logged and
  does not affect the node.
- Trace context crosses nodes: `ClientCommandRequest` and `ClientQueryRequest` carry a `headers` map. A follower
  that forwards a request opens `kurin.command.forward` and sends its headers; the leader continues that trace in
  `kurin.command` / `kurin.query`, and `kurin.raft.apply` on the leader is a child of the command that produced
  the entry (headers are remembered per log index until the entry is applied).
- MDC keys `kurin.node`, `kurin.term`, `kurin.role` are set on the Raft thread whenever the status changes, so
  they are present in every log line of that thread, including those of command handlers.
- `kurin-spring-boot-starter`: `KurinRaftMetrics` (a `KurinObserver` plus gauges for role, term, commit index,
  applied index and number of log entries), `ObservationInstrumentation` (spans as Micrometer observations with
  `SenderContext` / `ReceiverContext`, so any tracing bridge propagates through them), `KurinHealthIndicator`,
  `KurinEndpoint` (`/actuator/kurin`) with `KurinEndpointContributor` for sections of other starters. All of it is
  in `KurinObservabilityAutoConfiguration`, conditional on the classes and beans being present.
- Locks starter: counters and timers for lock acquisition and job claims and runs, a gauge of held locks,
  observation `kurin.lock.acquire`, a `coordination` section of the endpoint.
- Cache starter: `KurinCacheObserver` bean feeding meters for lookups per tier and result, loads, evictions,
  stampede waits and the state of the external store circuit; observations `kurin.cache.get` and
  `kurin.cache.load`; health `kurinCache`; a `caches` section of the endpoint.
- Routing starter: counters of connections per shard and role and of fallbacks, gauges of node state and of
  replica lag (with a `ReplicaPositionProbe`), a timer of fan-out calls, health `kurinRouting`.
- Test: `KurinObservabilityTest` checks meters, observations with their tags, MDC during apply, health in both
  states and the endpoint with a contributed section.

**Documentation (spec section 11)**

- `README.md` in the root: what Kurin is, what is replicated and what is local, guarantees and non-guarantees of
  commands, reads, time, leadership, locks, jobs, cache and routing, and the full configuration reference with
  types and defaults.
- `README.md` in each of the nine modules: usage with examples for every annotation, guarantees and limits of
  that module, extension points, meters.
- `CLAUDE.md` rewritten for the 1.1 architecture and threading model.
- `MIGRATION-1.1.md` completed with the observability changes.
- `docs/proposal.md`: "Zero-Copy serialization" replaced by what the code does (binary Kryo encoding with a
  type registry), "deterministic timeouts" replaced by randomized election timeouts with PreVote and
  check-quorum. `docs/0_intro.md` uses "deterministic consensus" in the general sense of the problem statement,
  not as a claim about timeouts, and is left as it is.

### Decisions and deviations

- **Micrometer is not a dependency of `core`.** The spec asks for metrics of the Raft core and forbids
  non-optional dependencies there, so the core exposes `KurinObserver` / `KurinInstrumentation` and the starter
  adapts them.
- **Health has three states.** `UP` with a known leader, `UNKNOWN` without one, `OUT_OF_SERVICE` after
  `kurin.node.health.no-leader-threshold`. A node that has just started or is in the middle of an election
  should not be taken out of rotation at once, and reporting `UP` without a leader would be false.
- **`kurinCache` is never `DOWN`.** The external tier is optional by design; an open circuit is reported as
  `degraded`.
- **`kurinRouting` goes `OUT_OF_SERVICE` only with `kurin.routing.health.primary-required=true`.** An
  application may prefer to keep serving reads from replicas while a primary is down.
- **No `/actuator/kurin` section for routing.** `KurinEndpointContributor` lives in the Kurin starter, and the
  routing starter deliberately does not depend on it (phase 7). Routing state is in its health details and
  meters.
- **Fan-out has a timer, not an observation.** The routing starter has no `KurinInstrumentation`, which belongs
  to the node. `kurin.routing.fanout.duration` and `kurin.routing.fanout.failed.shards` cover it.
- **No separate `kurin.routing.shard.requests`.** `kurin.routing.connections{shard,role}` counts the same events
  with one more tag.
- **`kurin.raft.rpc.latency` is tagged with the peer.** Clusters are small and static, so the cardinality is
  bounded by the peer list.
- **`kurin.raft.apply.duration` is tagged with the simple class name of the command.** The set of command types
  is fixed at start by the type registry.
- **The `management.health.<name>.enabled` properties are not honoured** by the Kurin health contributors; a bean
  with the same name replaces the built-in one. Using the Boot condition would make `spring-boot-actuator-autoconfigure`
  a compile dependency of every starter.
- **Lock and cache spans are reported through the node instrumentation**, so they exist only where a node does.
  Their tags are low-cardinality on purpose: the cache name, never the key.

### Verification

- `./mvnw clean install` after all phase 8 changes: BUILD SUCCESS, 124 tests in 9 modules, no failures.
- `grep -rn "TODO\|FIXME\|catch (Exception ignored)" --include=*.java` finds nothing; no `Thread.sleep` in the
  sources.
- **The 20-run stability loop was not completed for the suites of phases 7 and 8.** It was started for
  `kurin-spring-boot-starter` and for the two routing modules and was stopped by the environment (the machine ran
  low on memory) after 3 and 2 complete green runs. The suites of phases 1 to 6 passed 20 runs in a row when
  those phases were finished. To finish the check:
  `for i in $(seq 1 20); do ./mvnw -o -q -pl kurin-spring-boot-starter,kurin-data-routing,kurin-data-routing-spring-boot-starter surefire:test || break; done`

---

# Kurin 1.1.1

Work follows `KURIN_1.1.1_FIXES_CLAUDE_CODE_PROMPT.md` (fixes after the review of 1.1.0), phase by phase, on branch
`feature/kurin-1.1.1`.

| Phase | Scope | Status |
|-------|-------|--------|
| A | Services without snapshots (section 4), disk failure (section 3) | done |
| B | Spring Boot 3.2+ and 4.x compatibility, Actuator adapters, integration test modules (section 2) | done |
| C | Database access outside of a shard, migrations per shard, `db` health, Hibernate (section 1) | pending |
| D | Routing section of `/actuator/kurin`, fan-out observation, `diff` performance (section 5) | pending |
| E | Stability runs, release 1.1.1 (sections 6 and 7) | pending |

## 1.1.1, phase A

### Done

**Services without snapshots (section 4)**

- A service with `@KurinCommand` handlers and no `@KurinSnapshot` / `@KurinRestore` methods now fails the start of
  the node, with a message that names the service and the three ways out.
- `@KurinService(stateless = true)` declares a service whose commands keep nothing in its memory. `value` of the
  annotation got a default (empty: the class name), so that `stateless` can be given alone.
- `KurinNode.Builder.allowServicesWithoutSnapshot(true)` / `kurin.node.allow-services-without-snapshot=true` restores
  the old behaviour for all services, with a warning per service.
- A service declared stateless that has snapshot methods is refused as contradictory.
- The state machines of the framework were checked: `LockStateMachine`, `JobStateMachine` and
  `CacheInvalidationStateMachine` all have snapshot and restore methods.
- Tests: `KurinStartupValidationTest` (refused, allowed by property, contradictory declaration). Test services that
  kept state got snapshot methods; those that act only through cache annotations are declared stateless.

**Disk failure (section 3)**

- A node whose storage fails enters the state `STORAGE_FAILED`: `RaftStatus.storageFailure()` holds the reason,
  `storageFailed()` is true. A leader steps down; the node neither votes nor answers pre-votes, does not stand for
  election, refuses `AppendEntries` and `InstallSnapshot`, and fails commands and queries (local reads included)
  with the new `KurinStorageException`. Requests that were pending on it end with the same exception.
- Every access to the log, the snapshot store and the term/vote file goes through `GuardedStorage`. An
  `UncheckedIOException` from any of them is recorded and reaches the caller as `KurinStorageException`; the role
  changes when the running task ends (`RaftState.leaveClusterIfStorageFailed`), not in the middle of a handler.
- `flushLogIfIdle` no longer swallows a failed flush: it leads to the same state.
- Recovery runs on the Raft thread with exponential backoff, 100 ms doubling up to 10 s
  (`RaftConfig.storageRetryMinMs/MaxMs`, `kurin.node.storage-retry-min/max`). An attempt calls `RaftLog.recover()`,
  saves term and vote again and calls `SnapshotStore.verifyWritable()`; when all three succeed the node returns as
  a follower.
- `KurinObserver.onStorageFailure(cause)` / `onStorageRecovered()`, gauge `kurin.raft.storage.failed`, health
  `kurin` is `DOWN` with detail `storageFailure`, `/actuator/kurin` shows the reason.
- `KurinNode.Builder.storageDecorator(...)` with the record `RaftStorage(log, snapshots, metadata)`; in `kurin-test`
  `FaultInjectingStorage` and `KurinTestCluster.breakStorage / repairStorage / storageFaults`.
- Tests: `RaftStorageFailureTest` (leader with a failed disk yields, two healthy nodes carry on, the node returns
  and catches up; a failed follower does not hold back commits; a failed snapshot store; file log rewritten after
  repair and read back after a restart), `FileRaftLogTest.recoverRewritesDamagedFilesFromMemory`,
  `KurinObservabilityTest.failedStorageIsReportedAsDownAndClearsAfterRepair`.

### Decisions and deviations

- **Recovery rewrites the log; it does not just flush again.** The spec says "retry `sync`". After a failed
  `fsync` the operating system may have dropped the dirty pages, so a later `fsync` that succeeds proves nothing
  about them, and a failed append can leave a torn record. `RaftLog.recover()` therefore writes the entries held
  in memory to a fresh segment, flushes it and puts it in place of the old files. For a log that lives in memory
  the default `recover()` is `sync()`, as the spec describes.
- **`STORAGE_FAILED` is a field of `RaftStatus`, not a fourth `RaftRole`.** The spec asks for a field. The node
  reports the role `FOLLOWER`, so code that switches over the three roles keeps working; `RaftStatus` kept its
  eight-argument constructor.
- **A failed node does not adopt a higher term.** It could not persist it. It keeps the term it had and adopts the
  current one from the first `AppendEntries` after it returns.
- **A storage failure during start still fails the start.** There is no role to leave yet, and starting a node
  that cannot read or write its state would only hide the problem.
- **`FilePersistentMetadata.save` now throws `UncheckedIOException`** instead of `IllegalStateException`, so that
  all three parts of the storage report I/O failures the same way.
- **`AT_MOST_ONCE` semantics of commands are unchanged.** A command refused with `KurinStorageException` before it
  was appended was not applied; one that was already in the log has an unknown outcome and can be retried with
  the same request id, exactly as after a leader change.

## 1.1.1, phase B

### Done

- **Audit** in `docs/boot-compatibility.md`: every class of Spring Boot and Spring Framework the modules use, where
  it is in Spring Boot 3.2.12 and in 4.1.1, and how a difference is handled. The facts were read from the jars
  (`jar tf`, `javap`) of both versions; 4.1.1 was the latest stable 4.x on Maven Central.
- **The starters are neutral.** They refer to autoconfigurations of Spring Boot only by name, with the Spring
  Boot 3 and the Spring Boot 4 name where the class moved (metrics, observation, data source). They no longer
  depend on `spring-boot-starter-aop`, which Spring Boot 4 renamed, but on `spring-aop` and `aspectjweaver`.
- **Health verdicts moved out of Spring**: `KurinNodeHealth` (`core`), `KurinCacheHealth` (`kurin-tiered-cache`),
  `KurinRoutingHealth` (`kurin-data-routing`), and the bean `KurinRoutingDiagnostics` in the routing starter. The
  starters contain no class of the health API any more.
- **`kurin-actuator-boot3` and `kurin-actuator-boot4`**: health indicators `kurin`, `kurinCache`, `kurinRouting`,
  the composite `db` of the physical pools (section 1.3), and the `routing` section of `/actuator/kurin` (section
  5.1). Each imports the BOM of its generation at module level. Every indicator honours
  `management.health.<name>.enabled` and `management.health.defaults.enabled` through
  `@ConditionalOnEnabledHealthIndicator` of its generation, and yields to a bean of the same name. This closes
  the deviation of 1.1.0 phase 8.
- **Build**: the default build has all 14 modules, including `kurin-actuator-boot4` and `kurin-it-boot4` on the
  Spring Boot 4 BOM. Profile `boot4` (`./mvnw -Pboot4 clean install`) switches the BOM of the starters to 4.1.1 and
  runs their tests on it; the modules bound to one generation keep theirs. Surefire is 3.5.5, which runs both
  JUnit 5 (Spring Boot 3) and JUnit 6 (Spring Boot 4). `jitpack.yml` is unchanged: `./mvnw clean install
  -DskipTests` builds everything on Java 21.
- **`kurin-it-boot3` and `kurin-it-boot4`** compile and run the same sources (`kurin-it/src`): an application with
  a replicated counter, a distributed lock, a `@KurinScheduledOnce` job, a cache on L1 + L2 + shared in-memory L3
  with `@KurinCacheEvict` in a transaction, and routing over two shards with a replica each. The test starts
  three instances with web servers in one JVM and checks over HTTP: one leader and commands from any instance,
  the job running on one instance per slot, the lock admitting one instance at a time, invalidation reaching all
  instances after commit, read-your-writes through the token header across instances, `/actuator/health` UP with
  all Kurin contributors, `/actuator/kurin` with all sections, and the meters of every module. They are not
  installed or deployed (`maven.install.skip`, `maven.deploy.skip`).
- Tests: `KurinActuatorAdapterTest` in both adapter modules (11 tests each).

### Verification

| Build | Starters compiled and tested on | Result |
|---|---|---|
| `./mvnw clean install` | 3.2.12 | green, 169 tests |
| `./mvnw -Pboot4 clean install` | 4.1.1 | green (155 tests before the integration modules had tests; repeated at the end of phase E) |
| `./mvnw -Dspring-boot.version=3.5.16 -Dkurin.boot3.version=3.5.16 clean install` | 3.5.16 | green, 169 tests |

In the default build the starter jars compiled against 3.2.12 run on 4.1.1 in `kurin-actuator-boot4` and
`kurin-it-boot4`; with `-Pboot4` the jars compiled against 4.1.1 run on 3.2.12 in `kurin-actuator-boot3` and
`kurin-it-boot3`. Both directions pass. Spring Boot 3.3 and 3.4 were not run separately: they lie between two
lines that pass, and the API Kurin uses did not change inside the 3.x line.

### Decisions and deviations

- **Only health needed adapters.** The spec allows moving the endpoint too, if the endpoint API changed. It did
  not: `@Endpoint` and `@ReadOperation` have the same names and attributes in both generations, so
  `/actuator/kurin` and `KurinEndpointContributor` stay in the node starter.
- **The health verdicts live in the plain modules, not in the starters** as the spec suggests
  (`KurinHealthSnapshot`). They need nothing of Spring, and the routing starter does not depend on the node
  starter, so there is no starter all three could share.
- **A tolerated dead database node is reported `UP` with `degraded: true` and `state: DOWN`** in the `db`
  composite. A composite takes the worst status of its parts, so a part that reported `DOWN` would take the
  instance out of the load balancer for a dead replica, which is exactly what section 1.3 is about.
- **With `kurin.routing.health.enabled=false` the `db` health asks the pools directly**, as Spring Boot would,
  because then nobody watches them.
- **Data sources that are not behind the routing keep their ordinary health** inside the same `db` composite.
  Replacing the contributor of Spring Boot must not make them disappear.
- **The integration tests talk plain text and match JSON by substring.** Spring Boot 3 ships Jackson 2 and
  Spring Boot 4 Jackson 3, in different packages and with a different field order; a JSON library in the shared
  test sources would tie them to one generation.
- **JPA and Liquibase in the integration application come with phase C**, which makes them work on two shards.
