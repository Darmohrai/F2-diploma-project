# kurin-actuator-boot4

Health indicators of Kurin for the Actuator of **Spring Boot 4.x**. For Spring Boot 3.2 - 3.5 use
[kurin-actuator-boot3](../kurin-actuator-boot3/README.md); the two differ only in the health API they are compiled
against.

```xml
<dependency>
    <groupId>org.kurin</groupId>
    <artifactId>kurin-actuator-boot4</artifactId>
    <version>1.1.2</version>
</dependency>
```

Each indicator appears when the Kurin module it reports on is in use.

| `/actuator/health` entry | Needs | Reports | Switch |
|---|---|---|---|
| `kurin` | node starter | `UP` with a known leader; `UNKNOWN` during an election; `OUT_OF_SERVICE` after `kurin.node.health.no-leader-threshold` without a leader; `DOWN` while the node cannot write its durable state | `management.health.kurin.enabled` |
| `kurinCache` | cache starter | always `UP`; `degraded: true` while the circuit of an external store is not closed | `management.health.kurin-cache.enabled` |
| `kurinRouting` | routing starter | `UP`; `degraded: true` while a database node is down; `OUT_OF_SERVICE` if a primary is down and `kurin.routing.health.primary-required=true` | `management.health.kurin-routing.enabled` |
| `db` | routing starter | one entry per physical pool, `<shard>.primary` and `<shard>.replica`, plus every data source of the application that is not behind the routing | `management.health.db.enabled` |

A bean with the name of an indicator (`kurinHealthIndicator`, `kurinCacheHealthIndicator`,
`kurinRoutingHealthIndicator`, `dbHealthContributor`) replaces it.

## What `kurin: UP` means, and `commandPath`

`UP` means that the instance is a working member of its cluster: it leads, or it follows a leader it hears from.
It does not mean that a command submitted on this instance gets through at this very moment. A follower hears
its leader over a connection the leader opened, and sends its own commands and linearizable queries over a
connection it opens itself. That second fact is the detail `commandPath`:

| `commandPath` | Meaning |
|---|---|
| `UP` | The instance is the established leader, or has an open connection to the leader. |
| `CONNECTING` | A leader is known and nothing is known to be wrong, but the connection is not open: it is being opened, or will be by the first request that needs it. Idle connections are closed after `kurin.node.idle-timeout`, so a follower that forwards nothing shows this most of the time. A request submitted now waits for the connection. A leader shows it between its election and the commit of the first entry of its term. |
| `DOWN` | No leader is known, the node is not started or its storage failed, or the last attempt to connect to the leader failed and the next one is not due yet. |

The status is deliberately not derived from `commandPath`: an instance that follows a leader serves local reads
and everything that needs no cluster, and taking it out of rotation because an idle connection was closed would
make every quiet follower flap. If an instance must not receive traffic until its commands get through, use the
detail in a readiness check of your own (`KurinNode.commandPath()` gives the same value). `kurin: UP` with
`commandPath: DOWN` for longer than a few seconds says that the leader can reach this instance and not the other
way round.

## Why `db` is replaced

Spring Boot builds its `db` health from every `DataSource` bean by asking it for a connection. The routing data
source cannot give one without knowing the shard, so with more than one shard that check fails for ever, the
overall health is `DOWN` and a load balancer takes every instance out. The replacement reports the pools the
connections really come from:

- a pool is read from the health monitor of the routing, so a health request opens no connection;
- a dead replica is `UP` with `state: DOWN` and `degraded: true`: its reads are served by the primary, and the
  instance must stay in service;
- a dead primary is the same unless `kurin.routing.health.primary-required=true`, then `OUT_OF_SERVICE`;
- with `kurin.routing.health.enabled=false` nobody watches the pools, and each is asked directly, as Spring Boot
  would.

## `/actuator/kurin`

The endpoint itself belongs to the node starter. This module adds the section `routing` to it when the routing
starter is used as well: shards with weights, the state of every node, the replica lag if a
`ReplicaPositionProbe` is available, and the placement settings.

What an indicator reports is decided by classes that know nothing of Spring Boot (`KurinNodeHealth`,
`KurinCacheHealth`, `KurinRoutingHealth`); this module only translates.
