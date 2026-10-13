# core

An embedded Raft node: leader election, a replicated log, snapshots, and a state machine built from annotated
plain objects. Netty for transport, Kryo for the wire format. No Spring.

```java
public record Deposit(String account, long amount) { }
public record Balance(String account) { }

@KurinService("accounts")
public class Accounts {
    private final Map<String, Long> balances = new HashMap<>();

    @KurinCommand                       // goes through the log; runs on every node, in the same order
    public long deposit(Deposit command) {
        return balances.merge(command.account(), command.amount(), Long::sum);
    }

    @KurinQuery                         // never goes through the log; must not change state
    public long balance(Balance query) {
        return balances.getOrDefault(query.account(), 0L);
    }

    @KurinSnapshot                      // the whole state of this service, in a format of its own choice
    public byte[] snapshot() {
        return encode(balances);
    }

    @KurinRestore                       // replaces the state with a snapshot taken by the method above
    public void restore(byte[] snapshot) {
        balances.clear();
        balances.putAll(decode(snapshot));
    }
}

KurinNode node = KurinNode.builder()
        .localNode("10.0.0.11", 9400)
        .addPeers("10.0.0.12:9400", "10.0.0.13:9400")
        .dataDir(Path.of("/var/lib/app/kurin"))
        .addService(new Accounts())
        .build();
node.start();

long total = node.<Long>submitCommand(new Deposit("a", 10)).get();
long seen = node.<Long>submitQuery(new Balance("a"), ReadConsistency.LINEARIZABLE).get();
```

A handler takes exactly one parameter; its class routes the message, so a command class has one handler in the
whole node. Any node accepts commands and queries: a follower forwards them to the leader.

## Annotations

| Annotation | On | Meaning |
|---|---|---|
| `@KurinCommand` | method | Handler of a replicated command. Must be deterministic. |
| `@KurinQuery` | method | Read-only handler, answered by the leader (or locally with `ReadConsistency.LOCAL`). |
| `@KurinSnapshot` / `@KurinRestore` | method | Capture the state of a service as `byte[]` (no parameters) and replace it from one (`byte[]` parameter). One of each per service. |
| `@KurinService("name")` | class | Stable name of the service in snapshots; without it the class name is used, and renaming the class orphans its snapshot. |
| `@KurinType(id = N)` | class | Explicit wire id (`N >= 1000`) of a type; without it the id is derived from the class name, and renaming the class changes it. |

## Guarantees

- A command acknowledged to the caller is on disk on a majority and is never lost while a majority survives.
- Commands are applied in log order on every node, each at most once per request id.
- `LINEARIZABLE` queries see every command acknowledged before the query started.
- A leader that cannot reach a majority steps down within one election timeout and stops answering.
- Persistent state (term, vote, log, snapshots) is checksummed; a torn tail is cut off at start, damage in the
  middle refuses to start.

## Not guaranteed

- After `KurinTimeoutException` the command may or may not have been applied. Retry with the same
  `CommandOptions.requestId`.
- Deduplication remembers a bounded number of request ids (`RaftConfig.dedupCacheSize`).
- Membership is static: the peer list is fixed at start. Changing it means restarting all nodes with the new list.
- Handlers that are not deterministic make nodes diverge silently. Use `KurinCommandContext.current()` for the log
  index, term, timestamp and request id of the command being applied.
- With `storage(MEMORY)` nothing survives a restart, and a restarted node can vote twice in a term. Tests only.

## Errors

All failures are subclasses of `KurinException`:

| Exception | Meaning | Outcome of the command |
|---|---|---|
| `KurinNotStartedException` | the node is not running | not applied |
| `KurinNoLeaderException` | no leader known within the timeout | not applied |
| `KurinNotLeaderException` | leadership moved while the request was in flight (retried internally where safe) | unknown |
| `KurinTimeoutException` | no answer in time | unknown |
| `KurinCommandFailedException` | the handler threw; the same on all nodes | applied, failed |
| `KurinSerializationException` | a type is not registered or cannot be encoded | not applied |
| `KurinRemoteException` | the other node failed to process a request | unknown |

## Threading

All Raft state lives on one thread, `kurin-raft-<port>`. Command and query handlers, state machine listeners,
role change listeners and `KurinObserver` callbacks run on it: they must not block. Netty threads only decode and
hand over. Callers get a `CompletableFuture`; do not block in its synchronous callbacks either.
With `-Dkurin.debug.thread-checks=true` every access to the state from another thread throws.

## Types on the wire

Kryo runs with registration required. Types of handler parameters, results and record components are discovered
and registered automatically; anything else (a subclass stored in a field of type `Object`, an element of a
collection) is added with `registerType`. Ids are stable across nodes and restarts, independent of registration
order. Nodes exchange a fingerprint of their registries at connect and refuse a mismatch, so a rolling upgrade
that changes command types has to keep both versions of a type registered until all nodes are upgraded.

## Observing

`KurinObserver` (elections, leader changes, commit latency, apply duration, RPC latency, batch sizes, snapshots)
and `KurinInstrumentation` (spans around commands, queries, forwarding and apply, with headers that travel to the
leader) are plain interfaces with no dependencies. The Spring starter bridges them to Micrometer.

Security: `NetworkConfig` supports mutual TLS and a shared-secret HMAC handshake. Without either, anything that
can reach the port can submit commands.
