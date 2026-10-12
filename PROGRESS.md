# Kurin 1.1.0 — progress log

Work follows `KURIN_1.1_CLAUDE_CODE_PROMPT.md` phase by phase. Each phase ends with a green
`./mvnw clean install` and a commit on branch `feature/kurin-1.1`.

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Build and modules, type registry, single-threaded Raft state, timers, step-down, response envelope, apply semantics, identity, network | done |
| 2 | NOOP entry, check-quorum, lease, PreVote, read consistency, leadership API, fault injection tests | done |
| 3 | Snapshots, compaction, file persistence | done |
| 4 | Spring integration of the core | done |
| 5 | Locks, leadership tasks, exactly-once jobs | pending |
| 6 | Cache | pending |
| 7 | Data routing | pending |
| 8 | Observability, documentation | pending |

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
