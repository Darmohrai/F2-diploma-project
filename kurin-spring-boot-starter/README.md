# kurin-spring-boot-starter

Runs a Kurin node inside a Spring Boot application.

```properties
kurin.node.advertised-host=10.0.0.11
kurin.node.port=9400
kurin.node.peers=10.0.0.11:9400,10.0.0.12:9400,10.0.0.13:9400
```

Every bean with `@KurinCommand` / `@KurinQuery` methods becomes a service of the node:

```java
@Component
@KurinService("feature-flags")
public class FeatureFlags {
    private final Map<String, Boolean> flags = new HashMap<>();

    @KurinCommand
    public boolean set(SetFlag command) {
        flags.put(command.name(), command.enabled());
        return command.enabled();
    }

    @KurinQuery
    public boolean isEnabled(IsEnabled query) {
        return flags.getOrDefault(query.name(), false);
    }
}

@Service
public class FlagAdmin {
    private final KurinTemplate kurin;

    public void enable(String name) {
        kurin.submitCommand(new SetFlag(name, true)).join();      // same on every instance once it returns
    }

    public boolean check(String name) {
        return kurin.<Boolean>query(new IsEnabled(name), ReadConsistency.LEASE).join();
    }
}
```

Service beans must be eagerly created singletons; a service bean that appears after the node started (`@Lazy`,
global lazy initialization) fails with an explanation. The node calls the target object directly on its own
thread, not through Spring proxies: `@Transactional`, `@Async` and other advice on a handler have no effect there.

## Lifecycle

- The node starts as a `SmartLifecycle` after all singletons exist, so every service and type is registered
  before the first log entry is replayed. It stops before the beans it calls are destroyed.
- `KurinLeaderElectedEvent` / `KurinLeaderLostEvent` are published on a separate thread, not on the Raft thread.
  An "elected" event means the node has established its term (its first entry is committed).
- A node with no quorum still starts; the application comes up and reports its state through health.

## Extension points

| Bean | Called | For |
|---|---|---|
| `KurinNodeBuilderCustomizer` | before the node is built | settings not covered by properties, observers, the transport |
| `KurinNodeCustomizer` | after it is built, before start, with the discovered services | adding state machines and listeners (used by the lock and cache starters) |
| `KurinTypeRegistrar` | while types are collected | registering wire types in code |
| `KurinEndpointContributor` | on `/actuator/kurin` | a named section of the endpoint |

## Observability

Active when the corresponding library is on the classpath and its registry bean exists.

Meters (`MeterRegistry`):

| Meter | Type | Tags |
|---|---|---|
| `kurin.raft.role` | gauge: 0 follower, 1 candidate, 2 leader | |
| `kurin.raft.term`, `kurin.raft.commit.index`, `kurin.raft.applied.index`, `kurin.raft.log.entries` | gauge | |
| `kurin.raft.elections` | counter | `result` = `started`, `won`, `lost` |
| `kurin.raft.leader.changes` | counter | |
| `kurin.raft.commit.latency` | timer: append on the leader to commit | |
| `kurin.raft.apply.duration` | timer | `type` = command class |
| `kurin.raft.rpc.latency` | timer | `type`, `peer` |
| `kurin.raft.append.batch.size` | distribution summary | |
| `kurin.raft.snapshot.duration`, `kurin.raft.snapshot.bytes` | timer, summary | |

Observations (`ObservationRegistry`): `kurin.command`, `kurin.query`, `kurin.command.forward`, `kurin.raft.apply`,
each tagged with the message type. With a tracing bridge, trace context travels in request headers from a follower
to the leader, so the leader's spans join the caller's trace.

Actuator:

- health `kurin`: `UP` with a known leader; `UNKNOWN` while there is none; `OUT_OF_SERVICE` once that lasts longer
  than `kurin.node.health.no-leader-threshold`. Details: role, term, leader, commit and applied index, peers with the
  time since last contact.
- `/actuator/kurin` (expose it with `management.endpoints.web.exposure.include=kurin`): the node section plus the
  sections contributed by other starters (`coordination`, `caches`).

Log lines written on the Raft thread carry MDC keys `kurin.node`, `kurin.term`, `kurin.role`.

## Configuration

`kurin.node.*` and `kurin.serialization.types`: see the [configuration reference](../README.md#configuration-reference).
Startup fails with a message naming the property when the configuration cannot work: no port, a non-loopback
cluster without `advertised-host`, an election timeout too close to the heartbeat.
