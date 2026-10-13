# kurin-locks-spring-boot-starter

Annotations for [kurin-locks](../kurin-locks/README.md). Brings in `kurin-spring-boot-starter`.

## `@KurinDistributedLock`

```java
@KurinDistributedLock(key = "'invoice:' + #invoiceId", ttl = "PT30S", onBusy = OnBusy.THROW)
public void settle(String invoiceId) {
    long token = KurinLockContext.current().fencingToken();
    ...
}

@KurinDistributedLock(key = "'import'", ttl = "PT1M", waitTimeout = "PT10S", onBusy = OnBusy.WAIT, autoRenew = true)
public Summary runImport() { ... }
```

| Attribute | Default | Meaning |
|---|---|---|
| `key` | - | SpEL over the arguments (`#name`, `#a0`, `#p0`). A constant must be quoted. An expression that fails or yields null raises `KurinLockKeyException`; it is never used as a literal. |
| `ttl` | `PT30S` | Lease of the lock, not the duration of the method. |
| `waitTimeout` | `PT0S` | How long to wait for a busy lock. |
| `onBusy` | `SKIP` | `SKIP`: do not run, return null or an empty `Optional` (refused at start for primitive return types). `THROW`: `KurinLockAcquisitionException`. `WAIT`: wait up to `waitTimeout`, then throw. |
| `autoRenew` | `false` | Extend the lease every third of the ttl while the method runs. |
| `reentrant` | `false` | The thread that holds the key enters again. |

When the cluster cannot answer, the method does not run and a `KurinException` is thrown, whatever `onBusy` says.

## `@KurinLeaderOnly`

```java
@Scheduled(fixedDelay = 5000)
@KurinLeaderOnly                                   // other instances skip
public void compact() {
    long term = KurinLeadership.currentToken().orElseThrow().term();
    ...
}

@KurinLeaderOnly(onNotLeader = OnNotLeader.THROW)  // KurinNotLeaderException names the known leader
public Result adminAction() { ... }
```

Runs only where the node holds a valid leader lease at the start of the call. A call that outlives the lease can
overlap with the next leader's.

## `@KurinLeaderTask`

```java
@KurinLeaderTask(name = "outbox-relay", interval = "PT2S")
public void relay(LeadershipToken token, CancellationSignal signal) { ... }
```

A background loop on the leader; needs no `@Scheduled`. Parameters: none, a `LeadershipToken`, or a token and a
`CancellationSignal`.

## `@KurinScheduledOnce`

```java
@Scheduled(cron = "0 0 3 * * *")
@KurinScheduledOnce(name = "nightly-report", slot = "P1D", mode = JobMode.AT_LEAST_ONCE, lease = "PT30M")
public void nightlyReport() {
    long token = KurinJobContext.current().fencingToken();
    ...
}
```

Every instance fires the schedule; one runs per slot. Set `slot` to the period of the schedule. `AT_MOST_ONCE`
(default) never repeats a slot, even after a crash; `AT_LEAST_ONCE` repeats it when the winner does not finish
within `lease`, so the method has to tolerate a second run. **Instance clocks must agree to well within half a
slot.**

## Programmatic use

`KurinLockManager`, `KurinLeaderTasks`, `KurinJobs` are beans. `KurinLockTemplate` wraps the common shapes:

```java
lockTemplate.executeWithLock("invoice:" + id, Duration.ofSeconds(30), () -> settle(id));
Optional<Report> report = lockTemplate.tryExecuteWithLock("report", Duration.ofMinutes(1), this::build);
```

## Startup checks

Annotations are validated when the context starts, not at the first call: unparsable durations, `SKIP` on a
primitive return type, `WAIT` without a wait timeout, duplicate task names, unsupported parameters of a leader
task. Leader tasks start only after the node has started.

## Configuration, metrics

`kurin.locks.*`, `kurin.jobs.*`: see the [configuration reference](../README.md#configuration-reference).

| Meter | Type | Tags |
|---|---|---|
| `kurin.locks.acquire` | counter | `result` = `acquired`, `busy` |
| `kurin.locks.wait.duration` | timer | |
| `kurin.locks.held` | gauge: locks held by this instance | |
| `kurin.jobs.claims` | counter | `job`, `result` = `granted`, `denied` |
| `kurin.jobs.duration` | timer | `job`, `status` = `succeeded`, `failed` |

Observation `kurin.lock.acquire` covers acquisition including the wait. `/actuator/kurin` gets a `coordination`
section with the number of locks and the recent runs of known jobs.
