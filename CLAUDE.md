# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 1.1 rework in progress

Branch `feature/kurin-1.1` implements `KURIN_1.1_CLAUDE_CODE_PROMPT.md` phase by phase. `PROGRESS.md` is the source of truth for what is done and why; `MIGRATION-1.1.md` lists breaking changes. Sections below that contradict `PROGRESS.md` describe 1.0.4 and are rewritten in phase 8.

## What this is

Kurin is a Java 21 infrastructure framework (a university qualification project): an embedded Raft consensus core on Netty + Kryo, with tiered caching and distributed locks layered on top, each exposed through a Spring Boot starter. Project documentation in `docs/` is written in Ukrainian (several chapters are still empty stubs); log messages and code comments mix English and Ukrainian.

## Commands

Maven multi-module build via the wrapper (`mvnw.cmd` on Windows, `./mvnw` elsewhere). There is no lint/format step configured.

```
./mvnw clean install                 # build everything + run tests
./mvnw clean install -DskipTests     # what JitPack runs (jitpack.yml)
./mvnw -pl kurin-cache-spring-boot-starter -am test                          # one module plus the modules it depends on
./mvnw -pl kurin-cache-spring-boot-starter -am test -Dtest=CacheIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
./mvnw -pl kurin-spring-boot-starter -am test -Dtest=KurinStarterIntegrationTest#shouldProcessCommandViaSpringRaft -Dsurefire.failIfNoSpecifiedTests=false
```

`-am` matters: sibling modules are resolved from the reactor, otherwise from whatever stale version is in `~/.m2`.

Tests are `@SpringBootTest` integration tests that start a real single-node Raft cluster. They:
- bind fixed TCP ports (`kurin.node.port` = 9011, 9021) — a new test class needs its own unused port;
- leave state in the module's working directory: `raft-data-<port>/metadata.bin` (persisted term/vote) and `./kurin-data/cache` (RocksDB). Neither is in `.gitignore`. Stale state from a previous run can change behaviour (e.g. L2 cache hits surviving between runs);
- rely on `Thread.sleep` for leader election and for async cache writes.

Only the two Spring starters for core and cache have tests; `core`, `kurin-tiered-cache`, `kurin-locks` have none.

## Module layout and dependency direction

```
core                              Raft + Netty transport, no Spring at runtime
├─ kurin-spring-boot-starter      autoconfig: KurinNode bean, KurinTemplate, leader events
├─ kurin-tiered-cache             cache abstraction + Caffeine/RocksDB providers (plain Java)
├─ kurin-locks                    lock state machine + KurinLockManager (plain Java)
├─ kurin-cache-spring-boot-starter   → starter + tiered-cache + locks
└─ kurin-locks-spring-boot-starter   → starter + locks
```

The pattern is deliberate: each feature is a plain-Java library module plus a separate Spring starter holding the autoconfiguration and AOP aspects. Keep Spring types out of `core`, `kurin-tiered-cache`, and `kurin-locks`.

Dependency versions are hardcoded per module (Spring Boot 3.2.0 artifacts, no Boot BOM/parent); the root POM only manages Netty and SLF4J. All modules share the parent version (`1.0.4`) — bump them together.

## Core architecture (`core`)

**Entry point:** `KurinNode.builder()` → `RaftNode` wires `RaftState` + `RaftDispatcher` + `NetworkStarter`. `KurinNode.submitCommand` / `submitQuery` are the only public write/read paths.

**Threading model:** `RaftState` is not thread-safe and has no locks. All mutation is expected to happen as tasks on its `Mailbox` (single-thread executor created in `RaftNode`): client submissions, timer callbacks (Netty `Timer` → `mailbox.submit`), and RPC response callbacks all hop onto it. Inbound RPCs go through `AsyncThreadPoolDispatcher`, which serialises them on its *own* separate `Mailbox`. When adding code that touches `RaftState`, submit it to `raftState.getMailbox()` rather than calling directly from a Netty or caller thread.

**Roles as State pattern:** `FollowerRole` / `CandidateRole` / `LeaderRole` implement `NodeRole`; `RaftState.transitionTo` calls `onExit`/`onEnter` and fires `KurinRoleChangeListener`s on leadership change. Followers forward client commands and queries to the known leader over the network; queries are executed by the leader directly against the state machine without going through the log.

**State machine by annotation:** `PojoStateMachine` reflects over registered service objects and routes by *parameter type*:
- `@KurinCommand` / `@KurinQuery` methods must take exactly one parameter; that parameter's class is the routing key, so one command class maps to exactly one handler across all services (duplicates throw at startup).
- `@KurinSnapshot` (returns `byte[]`) / `@KurinRestore` (takes `byte[]`) are aggregated per service class name.
- `StateMachineListener.onCommandApplied` fires after every applied command on every node — this is the hook the cache module uses.

**Serialization:** `KryoSerializer` runs with `registrationRequired=true`. Framework classes have fixed IDs (10–29); user command/query classes get IDs from 100 upward **in registration order**, so every node must register the same classes in the same order. Any new RPC type must be added to `KryoSerializer` and to the sealed `RaftRpc` switch in `RaftDispatcher`. Types nested inside commands also need registration.

**Durability today:** only term/votedFor are persisted (`FilePersistentMetadata`, path derived from the port). `RaftNode` hardcodes `InMemoryRaftLog`; `FileRaftLog` exists but is not wired in. Request deduplication is an in-memory LRU of 10 000 request IDs in `RaftState`.

## Spring integration

Autoconfigurations are registered through `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` in each starter — a new autoconfig class needs a line there. Property prefixes: `kurin.node` (host/port/peers as `host:port`), `kurin.cache`, `kurin.locks`.

`KurinAutoConfiguration` scans every non-Spring bean for the `@Kurin*` annotations, registers command types with Kryo, and adds the beans as state-machine services. Feature starters extend the node without touching that class by contributing a `KurinNodeBuilderCustomizer` bean, which receives the builder and the list of discovered services (this is how the cache starter attaches `RaftCacheUpdater`). The locks starter's `DistributedLockService` is just another `@KurinCommand` bean picked up by the same scan.

## Cache

`KurinCacheManager.getOrCreateCache(name, tier)` picks a `CacheProvider` by `CacheTier`. `TIERED_SMART` builds a `TieredCompositeCache` of L1 Caffeine → L2 RocksDB → optional L3, where L3 is any user-supplied `CacheProvider` bean supporting `L3_EXTERNAL`, wrapped in a circuit-breaking `FaultTolerantCacheDecorator`. The tier is fixed by whoever creates a cache name first; later calls with a different tier get the existing instance.

Two independent paths populate a cache:
1. **Read-through (local, AOP):** `@KurinCacheable` → `KurinCacheAspect`. Supports SpEL keys (an expression that fails to parse is used as a literal key), null caching via the `KurinNullValue` sentinel, TTL jitter via `CacheEntry`, and stampede protection that takes a Raft-backed lock through `KurinLockManager` (optional dependency — degrades to unprotected if the locks starter is absent).
2. **Replicated write-through (Raft):** `@KurinCachePut` / `@KurinCacheEvict` placed on a `@KurinCommand` method. `RaftCacheUpdater` applies them in `onCommandApplied`, so every node updates its local cache when the command commits. The key comes from the annotation's `key`, else `CacheKeyProvider.getCacheKey()` on the command, else `toString()`.

`TieredCompositeCache.put` is asynchronous (`CompletableFuture.runAsync`), so a read immediately after a put may miss; reads backfill upper tiers on a lower-tier hit.

## Locks

Locks are replicated state: `KurinLockManager.tryLock/unlock` submit `AcquireLockCommand` / `ReleaseLockCommand` through Raft to `DistributedLockService`. Owner identity is `<node address>-<thread id>`, and re-acquiring by the same owner extends the TTL. `tryLock` returns `false` on any failure including timeout or no leader. `@KurinDistributedLock` and `@KurinLeaderOnly` (`KurinLocksAspect`) **silently return `null`** when the lock is held elsewhere / the node is not leader — don't use them on methods whose callers need a real return value.

## Database routing and sharding

Lives in `kurin-cache-spring-boot-starter` (Spring side) and `kurin-tiered-cache` (`api` annotations, `routing` package with the consistent-hash ring), and is independent of Raft. `KurinRoutingAutoConfiguration` activates only when the application declares a `KurinDataSourceTopology` bean (one or more named shards, each a Primary + optional Replica `DataSource`); it then publishes a `@Primary KurinRoutingDataSource`.

The target pool is chosen per connection request from two thread-locals in `KurinRoutingContext`:
- **type** — `@KurinReadOnly` (method or class) selects REPLICA. Nothing marks writes explicitly: a connection requested with no type selected counts as a write, goes to PRIMARY, and records the client in `SessionStickyManager`, after which that client's `@KurinReadOnly` calls go to PRIMARY for `kurin.routing.sticky-window-ms`. The client comes from the `KurinClientIdResolver` bean (default: servlet principal or existing session id; `null` means no stickiness).
- **shard** — set by `KurinRoutingAspect` for methods that have a `@KurinShardKey` parameter. The argument is either the key or an object with a `@KurinShardKey` field; `KurinShardRouter` maps it to a shard name by consistent hashing. With more than one shard, a connection requested without a selected shard throws.

`KurinRoutingAspect` is ordered ahead of the transaction interceptor on purpose — the connection is bound when the transaction starts, so routing must already be decided.

SpEL keys in `@KurinCacheable` reference arguments by name (`#id`), which needs `-parameters` (enabled in the root POM, and by Spring Boot's parent for users), or positionally (`#a0` / `#p0`).
