# Kurin 1.1.0 — progress log

Work follows `KURIN_1.1_CLAUDE_CODE_PROMPT.md` phase by phase. Each phase ends with a green
`./mvnw clean install` and a commit on branch `feature/kurin-1.1`.

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Build and modules, type registry, single-threaded Raft state, timers, step-down, response envelope, apply semantics, identity, network | done |
| 2 | NOOP entry, check-quorum, lease, PreVote, read consistency, leadership API, fault injection tests | pending |
| 3 | Snapshots, compaction, file persistence | pending |
| 4 | Spring integration of the core | pending |
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
