# kurin-locks

Coordination primitives on top of a Kurin node, without Spring: fenced locks, leader-only tasks, and jobs that
run once per time slot.

## Locks

```java
LockStateMachine locks = new LockStateMachine();
KurinNode node = KurinNode.builder()...addService(locks).build();
KurinLockManager manager = new KurinLockManager(node, locks, Duration.ofSeconds(3));

try (LockHandle lock = manager.lock("invoice:42", LockOptions.ttl(Duration.ofSeconds(30))
        .waitTimeout(Duration.ofSeconds(5)).autoRenew(true))) {
    storage.write(invoice, lock.fencingToken());       // the storage rejects tokens older than the last it saw
}

Optional<LockHandle> maybe = manager.tryLock("report", Duration.ofMinutes(1));   // empty when busy
```

- `tryLock` returns empty when the lock is busy; `lock` throws `KurinLockAcquisitionException` after the wait.
  Both throw a `KurinException` when the cluster cannot answer: "busy" and "unknown" are never confused.
- The owner is the handle, not the thread: a handle can be released from another thread. With
  `reentrant(true)` the thread that holds a key enters again locally.
- `fencingToken()` is the log index of the grant: strictly larger for every later grant of any key.
- Waiters are woken by the release of the lock, not by polling.

**What a lock is not.** It is a lease of `ttl` measured in log time. A holder that stalls for longer than the ttl
can overlap with the next holder. `isHeld()` turns false slightly before the lease can expire, and `autoRenew`
extends the lease every third of the ttl while the process is healthy, but neither can stop a thread that is
already inside the critical section. Where two writers must never both succeed, pass the fencing token to the
resource and have it reject smaller tokens.

Expiry is deterministic: all nodes agree on which locks are expired, because expiry is decided by log time when a
command is applied, never by a node's own clock.

## Leader tasks

```java
KurinLeaderTasks tasks = new KurinLeaderTasks(node);
tasks.register("rebalance", Duration.ofSeconds(10), (token, signal) -> {
    while (!signal.isCancelled() && hasWork()) {
        step(token.term());                             // the term fences against an older leader
    }
});
tasks.start();
```

A task runs repeatedly on the node that holds a valid leader lease, and on no other. When leadership is lost the
signal is cancelled and the thread interrupted; a task that ignores both keeps running beside the new leader's
copy, which is why the term is passed along.

## Run-once jobs

```java
JobStateMachine jobState = new JobStateMachine();     // registered on the node like the lock state machine
KurinJobs jobs = new KurinJobs(node, Duration.ofSeconds(3), 50);

Optional<Report> report = jobs.runOnce("nightly-report", Duration.ofDays(1), JobMode.AT_MOST_ONCE,
        Duration.ofMinutes(10), () -> build(KurinJobContext.current().fencingToken()));
```

Every instance calls `runOnce` at about the same time; the cluster grants the current slot to one of them.

| Mode | A slot runs | If the winner crashes |
|---|---|---|
| `AT_MOST_ONCE` | zero or one time | the slot is lost |
| `AT_LEAST_ONCE` | one or more times | another instance takes the slot over after `lease`; if the winner was only slow, both run |

The slot is the instance's wall clock rounded to the nearest multiple of the slot duration. Clocks of the
instances must agree to well within half a slot. `history(jobName)` returns the last runs with owner, fencing token,
status and timestamps.
