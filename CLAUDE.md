# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Kurin is a Java 21 infrastructure framework (a university qualification project): an embedded Raft node on
Netty + Kryo, with locks, leader tasks, run-once jobs, a tiered cache and database routing on top, each exposed
through a Spring Boot starter. Version 1.1.0.

- `README.md` and the per-module READMEs state the guarantees and the configuration reference. Keep them true
  when behaviour changes.
- `PROGRESS.md` records what the 1.1 rework did per phase and every deliberate deviation, with reasons.
  `MIGRATION-1.1.md` lists the breaking changes since 1.0.x.
- `docs/` is the diploma text, in Ukrainian. Code, comments and log messages are in English.

## Commands

Maven multi-module build via the wrapper (`mvnw.cmd` / `./mvnw`). No lint or format step.

```
./mvnw clean install                 # everything, with tests
./mvnw clean install -DskipTests     # what JitPack runs (jitpack.yml)
./mvnw -pl kurin-locks-spring-boot-starter -am test
./mvnw -pl kurin-test -am test -Dtest=RaftSafetyTest -Dsurefire.failIfNoSpecifiedTests=false
./mvnw -pl kurin-test -am test -Dtest='RaftSafetyTest#isolatedLeaderStepsDownAndLeasesNeverOverlap' -Dsurefire.failIfNoSpecifiedTests=false
./mvnw -o -pl kurin-test surefire:test      # rerun already compiled tests, e.g. in a loop to check stability
```

`-am` matters: without it sibling modules come from whatever is in `~/.m2`.

The build needs `<proc>full</proc>` (set in the root POM) on JDK 23+, and compiles with `-parameters` so SpEL can
use argument names. Mockito from the Boot BOM does not run on JDK 23: tests use hand-written stubs or
`java.lang.reflect.Proxy`.

## Rules that are easy to break

- **No Spring in `core`, `kurin-locks`, `kurin-tiered-cache`, `kurin-data-routing`.** Micrometer and Actuator are
  optional even in the starters (`@ConditionalOnClass`, `<optional>true</optional>`).
- **The framework is generic.** No domain code, no demo placement strategies, no concrete external-cache
  provider: applications plug in `ExternalCacheStore`, `ShardResolver`, `ReplicaPositionProbe`.
- **Tests are deterministic:** Awaitility instead of `Thread.sleep`, `TestPorts.freePort()` instead of fixed
  ports, `@TempDir` instead of the working directory.
- **No stubs:** no `TODO`/`FIXME`, no empty implementations, no swallowed exceptions. What cannot be done is
  written down in `PROGRESS.md`.
- Per-command logging is at DEBUG.
- Module POMs share one shape; versions come from the Spring Boot BOM imported by the parent. All modules carry
  the parent version: bump them together.

## Modules

```
core                                    Raft, transport, serialization. org.kurin.*
kurin-test                              in-process cluster with fault injection; holds the Raft tests
kurin-spring-boot-starter               node autoconfiguration, KurinTemplate, observability
kurin-locks                             lock / job state machines, KurinLockManager, KurinLeaderTasks, KurinJobs
kurin-locks-spring-boot-starter         @KurinDistributedLock, @KurinLeaderOnly, @KurinLeaderTask, @KurinScheduledOnce
kurin-tiered-cache                      tiers, CacheReader, invalidation state machine
kurin-cache-spring-boot-starter         @KurinCacheable, @KurinCacheEvict, @KurinCachePut
kurin-data-routing                      routing context, consistency tokens, health monitor, hash ring
kurin-data-routing-spring-boot-starter  KurinRoutingDataSource, aspects, servlet filter; no dependency on the node
```

Each feature is a plain library plus a starter with the autoconfiguration and aspects. Autoconfigurations are
listed in `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` of each starter.

## Core

**Entry point:** `KurinNode.builder()`; `submitCommand` / `submitQuery` are the public write and read paths.
`RaftNode` wires `RaftState`, `RaftDispatcher` and the transport.

**Threading.** `RaftState` is single-threaded and lock-free by construction: everything that touches it runs on
the `kurin-raft-<port>` thread. Timers, inbound RPCs, RPC responses and client submissions are all posted to that
thread; nothing calls `RaftState` directly from a Netty or caller thread. Tests run with
`-Dkurin.debug.thread-checks=true` (set by surefire), which makes any access from another thread throw, so a
violation fails tests instead of corrupting state now and then. Timers carry an epoch, and a callback from a
cancelled timer is dropped. Command handlers, state machine listeners and observers run on the Raft thread and
must not block; `KurinEventDispatcher` moves Spring events off it.

**Roles** (`raft/state/roles`): `FollowerRole` (includes PreVote and forwarding of client requests),
`CandidateRole`, `LeaderRole` (replication with flow control, check-quorum, lease, ReadIndex). All step-downs go
through `RaftState.stepDown`. A new leader appends a NOOP entry and serves reads and `leadership()` only after it
commits.

**Reads:** `ReadConsistency.LINEARIZABLE` (ReadIndex, skipped while the lease is valid), `LEASE`, `LOCAL`.

**Persistence:** `FileRaftLog` (segments, CRC per record, group commit; only entries below `getDurableIndex` count
for the leader's own match index), `FilePersistentMetadata`, `FileSnapshotStore`. Snapshots carry the service
states and the deduplication table and are installed in chunks.

**State machine:** `PojoStateMachine` routes by parameter type of `@KurinCommand` / `@KurinQuery` methods; one
handler per message class per node. `KurinCommandContext` (thread-local during apply) gives log index, term,
leader timestamp and request id; handlers must be deterministic and take time only from it.

**Serialization:** `KurinTypeRegistry` assigns stable ids (`@KurinType(id)` or a hash of the class name),
independent of registration order, and is frozen at start. Its fingerprint is part of the `Hello` handshake,
together with the cluster name and an optional HMAC. A new RPC type goes into `BuiltinTypes` and the sealed
`RaftRpc` switch in `RaftDispatcher`.

**Errors:** everything public throws a subclass of `KurinException`; across the wire failures travel in
`RpcResult`.

**Observability hooks:** `KurinObserver` and `KurinInstrumentation` (spans; their headers ride in
`ClientCommandRequest` / `ClientQueryRequest` to the leader). Micrometer bridges live in the starter.

## Spring starter

`KurinServiceRegistry` (a `BeanPostProcessor`) collects beans with Kurin handler methods as they are created and
registers their unwrapped targets. The node is a `SmartLifecycle` (phase 0): it starts when all singletons exist
and stops before them. Feature starters extend the node with `KurinNodeCustomizer` (after build, before start)
or `KurinNodeBuilderCustomizer`; things that need a running node start in a later phase (coordination: 100).

Annotation validation happens in `BeanPostProcessor`s at context start (`KurinCoordinationRegistry`,
`KurinCacheAnnotationValidator`): a wrong annotation fails the start, not the first call.

## Locks and jobs

`LockStateMachine` and `JobStateMachine` decide everything from log time (`KurinCommandContext.timestampMs()`),
never from a node's clock; the fencing token is the log index of the grant. `KurinLockManager` wakes waiters on
release events applied locally, undoes an acquisition whose outcome is unknown, and treats "busy" and "cluster
unavailable" as different outcomes (empty versus exception). Leader-only code checks `KurinNode.leadership()`,
which requires a valid lease, not just the leader role.

## Cache

`CacheEntry` carries its expiry through all tiers (`CaffeineTier`, `RocksDbTier`, `ExternalTier` over
`ExternalCacheStore` with a circuit breaker and striped ordered writes). `CacheReader` implements load-on-miss
with single flight (`LOCAL`, `CLUSTER`), stale-while-revalidate and `InvalidationEpochs`, which stop a load that
raced an eviction from writing back. Evictions replicate through `CacheInvalidationStateMachine`; contents never
do. On non-command methods `@KurinCacheEvict` runs after commit of the surrounding transaction.

The cache has its own codec (`KryoCacheCodec`, allow-list of packages) and must not depend on the node's
serializer: that dependency forms a bean cycle.

## Data routing

`KurinRoutingDataSource` sits behind a `LazyConnectionDataSourceProxy`, so the choice is made at the first
statement, when the read-only flag of the transaction is known. Order of rules: `@KurinPrimary`, read-write
transaction, `@KurinReadOnly` with the consistency strategy, then node health. `KurinRoutingContext` scopes nest
and restore. `ShardTransactionGuard` pins a transaction to one shard. `KurinRoutingAspect` is ordered ahead of
the transaction interceptor on purpose.

## Testing

Raft behaviour is tested in `kurin-test` (`RaftClusterTest`, `RaftSafetyTest`, `RaftPersistenceTest`,
`HandshakeTest`) on `KurinTestCluster`. Starters are tested with `ApplicationContextRunner`; multi-instance
Spring tests start several contexts in one JVM. Fields of AOP-proxied beans read as null in tests: go through
methods. A new test that waits for a leader should use `awaitLeader` and, before asserting on followers,
`awaitInSync`.
