# Changelog

## 1.1.2

Four defects found while an application was integrated with 1.1.1. No public signature changed; what callers
observe differently is described in [MIGRATION-1.1.md](MIGRATION-1.1.md), section "1.1.1 → 1.1.2". Each fix names
the tests that failed on 1.1.1 and pass now.

### Fixed

- **A lock request that the cluster could not answer was reported as "busy".** `KurinLockManager.tryLock`
  returned empty, and `lock` and `@KurinDistributedLock(onBusy = THROW)` threw `KurinLockAcquisitionException`,
  when there was no leader, a node was out of reach or no answer came in time, although the documentation
  promised that "busy" and "unknown" are never confused. Now empty means that another owner holds the lock, and
  a cluster failure is thrown as `KurinNoLeaderException`, `KurinRemoteException` or `KurinTimeoutException`
  with its cause, whatever `onBusy` says. With a wait, attempts without an answer are repeated until the wait is
  over and the exception of the last one is thrown. A claim of `@KurinScheduledOnce` without an answer still
  skips the run, now with a warning and its own counter.
  Tests: `KurinLockManagerOutcomeTest`, `LockClusterTest` (`tryLockThrowsWhenTheClusterHasNoLeaderInsteadOfReportingBusy`,
  `lockWithWaitKeepsTryingAndThenThrowsTheLastClusterError`, `acquisitionWithUnknownOutcomeLeavesNoLockBehind`,
  `undoingAnAttemptWithUnknownOutcomeDoesNotReleaseTheLockOfTheNextAttempt`),
  `JobAndLeaderClusterTest.claimWithoutAnAnswerSkipsTheRunAndIsReportedAsUnknown`, `UnknownOutcomeTest`,
  `LockOutcomeTest` (`kurin-it`).

- **A node that started before its peer could not forward commands to it for up to two seconds after the peer
  was elected.** Its first attempts to connect had failed, and during the pause before the next one every
  forwarded command and linearizable query failed at once with `KurinRemoteException: Node ... is unreachable,
  next connection attempt is delayed`, while the node followed the leader and reported health `UP`. Now a message
  from a peer ends the pause, a forwarded request waits for the connection within its own timeout and fails only
  if an attempt to connect fails, and all requests to one peer share one attempt.
  Tests: `PeerStartOrderTest`, `ConnectionManagerTest`, `CommandPathTest`, `LockOutcomeTest` (`kurin-it`).

- **`@KurinLeaderTask(interval = "${...}")` was rejected at startup as an invalid duration**, so the interval
  could not come from the configuration. Placeholders in `interval`, and in `ttl`, `waitTimeout`, `slot` and
  `lease` of the neighbouring annotations, are now resolved against the environment when the context starts; a
  number without a unit is milliseconds. A placeholder without a value, or a value that is not a duration,
  stops the startup and names the bean, the method and the attribute.
  Test: `DurationPlaceholderTest`.

- **Under load a database node was reported DOWN although the database worked.** The periodic check borrowed its
  connection from the application's pool and failed when the pool was exhausted, and a request that timed out
  waiting for the pool marked the node down as well. Reads then moved to the primary and the metrics showed a
  replica outage that had not happened. Now a failure is classified (`ConnectionFailureClassifier`): a pool
  without a free connection leaves the node up and is counted in `kurin.routing.pool.exhausted`; the request
  fails with the pool's exception and is not moved to the primary. The check uses a connection of its own
  (`DriverNodeProbe`, `kurin.routing.health.probe`), and a node goes out after
  `kurin.routing.health.failure-threshold` failed checks in a row (default 2) instead of one. A request that
  really cannot connect still takes the node out at once.
  Tests: `ExhaustedPoolIsNotADeadNodeTest`, `ConnectionFailureClassifierTest`, `HealthMonitorTest`,
  `NodeProbeTest`, `PoolExhaustionRoutingTest`, `NodeHealthSettingsTest`, `PostgresNodeHealthTest` (PostgreSQL in
  a container).

### Added

- `NodeProbe` (SPI of the node health check) with `DriverNodeProbe` and `PoolNodeProbe`; meters
  `kurin.routing.pool.exhausted{shard,role}` and `kurin.routing.node.transitions{shard,role,to}`; `failureKind`
  in the health details of a node that is down.
- `KurinNode.commandPath()` and the detail `commandPath` (`UP`, `CONNECTING`, `DOWN`) of the `kurin` health and
  of `/actuator/kurin`: whether what this node submits can reach the leader. The health status is unchanged.
- Meters `kurin.locks.acquire{result="unknown"}`, `kurin.locks.release.unconfirmed`,
  `kurin.jobs.claims{result="unknown"}`; `LockListener.onAcquireFailed` / `onReleaseUnconfirmed`,
  `JobListener.onClaimUnknown`.

## 1.1.1

Fixes after the review of 1.1.0. Breaking changes and how to adapt are in
[MIGRATION-1.1.md](MIGRATION-1.1.md), section "1.1.0 → 1.1.1".

### Fixed

- **Applications with several shards could not start or stay in service.** Whatever asked the routing data
  source for a connection before a shard could be chosen failed: Liquibase and Flyway, Hibernate reading JDBC
  metadata, and the `db` health of Spring Boot, which was `DOWN` for ever and took every instance out of the load
  balancer.
  - `kurin.routing.unrouted.mode` (`startup-only` by default, `fail`, `default-shard`) and
    `kurin.routing.unrouted.shard`.
  - Liquibase and Flyway migrations run on the primary of every shard (`kurin.routing.migrations.*`).
  - The `db` health reports the physical pools, `<shard>.primary` and `<shard>.replica`; a dead replica degrades
    and does not take the instance out of service.
  - In the mode `fail`, Hibernate is kept from reading JDBC metadata, with a clear error when no dialect is named.
- **A node whose disk failed went quiet instead of saying so.** It now leaves the cluster: the leader steps
  down, the node does not vote or accept entries, requests fail with `KurinStorageException`, health is `DOWN`.
  It retries with a backoff of 100 ms to 10 s, rewrites its log from memory and returns as a follower.
- **A service with command handlers and no snapshot methods lost its state after log compaction**, with only a
  warning at startup. It now fails the start, unless declared `@KurinService(stateless = true)` or allowed with
  `kurin.node.allow-services-without-snapshot=true`.
- Two tests of the cache starter relied on the JVM not pausing for a few hundred milliseconds.

### Added

- **Spring Boot 4.** The same jars run on Spring Boot 3.2 - 3.5 and on 4.x. Health indicators moved to
  `kurin-actuator-boot3` and `kurin-actuator-boot4`; they honour `management.health.<name>.enabled`.
  `./mvnw -Pboot4 clean install` builds and tests the starters on Spring Boot 4. See
  [docs/boot-compatibility.md](docs/boot-compatibility.md).
- `kurin-it-boot3`, `kurin-it-boot4`: the same three-instance application tested on both generations.
- `/actuator/kurin` has a `routing` section.
- Observation `kurin.routing.fanout` with a child per shard; gauge `kurin.raft.storage.failed`;
  `KurinObserver.onStorageFailure` / `onStorageRecovered`.
- `KurinNodeHealth`, `KurinCacheHealth`, `KurinRoutingHealth`, `KurinRoutingDiagnostics`: health verdicts as
  plain classes, usable without Actuator.
- `KurinNode.Builder.storageDecorator`, `FaultInjectingStorage` and `KurinTestCluster.breakStorage` /
  `repairStorage` in `kurin-test`.
- `scripts/stability.sh`, `scripts/stability.ps1` and the Maven profile `stability` for repeated test runs.

### Changed

- The starters depend on `spring-aop` and `aspectjweaver` instead of `spring-boot-starter-aop`.
- `KurinHealthIndicator` and the health beans of the cache and routing starters are gone from the starters; add
  an Actuator adapter.

## 1.1.0

A rework of the framework for correctness. Breaking changes are in [MIGRATION-1.1.md](MIGRATION-1.1.md); what
was done and why, phase by phase, is in [PROGRESS.md](PROGRESS.md).

- **Core**: Raft state confined to one thread; timers with epochs; a single `stepDown`; NOOP entry of a new
  leader, check-quorum, leader lease, leader stickiness, PreVote; `LINEARIZABLE`, `LEASE` and `LOCAL` reads;
  flow control; snapshots with the deduplication table, installed in chunks; a segmented, checksummed file log
  with group commit; durable term and vote; a type registry with stable ids and a handshake that refuses
  mismatched registries; HMAC authentication and mutual TLS between nodes; a hierarchy of `KurinException`.
- **Spring starter**: services collected as beans are created, the node as a `SmartLifecycle`, validation of
  configuration and annotations at startup.
- **Locks**: fenced locks with waiters woken by release, compensation of acquisitions with an unknown outcome,
  reentrancy, auto-renewal; leader-only code checked against the lease; leader tasks; run-once jobs per slot
  with at-most-once and at-least-once modes.
- **Cache**: expiry carried through memory, disk and external tiers; an `ExternalCacheStore` SPI with a circuit
  breaker and ordered writes; single flight per instance or per cluster; stale-while-revalidate; evictions
  replicated through Raft and protected against write-back races; `@KurinCacheEvict` after commit.
- **Data routing** in modules of its own: read/write splitting decided at the first statement, read-your-writes
  across instances through a signed token, node health with fallback, weighted consistent hashing with a
  resharding plan, fan-out over shards, one shard per transaction.
- **Observability**: Micrometer meters and observations, trace context forwarded to the leader, Actuator health
  and `/actuator/kurin`, MDC on the Raft thread.
- **`kurin-test`**: an in-process cluster with partitions, delays, drops, kills and restarts.
- Documentation of guarantees and non-guarantees in the root and module READMEs.
