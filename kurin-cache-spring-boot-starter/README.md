# kurin-cache-spring-boot-starter

Annotations for [kurin-tiered-cache](../kurin-tiered-cache/README.md), with evictions replicated to every
instance through the Kurin node. Brings in `kurin-spring-boot-starter`.

## `@KurinCacheable`

```java
@KurinCacheable(cacheName = "accounts", key = "#id", ttl = "PT10M", jitterPercent = 10)
public Account find(String id) { ... }

@KurinCacheable(cacheName = "profiles", key = "#id", cacheNulls = true, nullTtl = "PT30S",
        staleWhileRevalidate = "PT1M", stampede = StampedeMode.CLUSTER, onLockTimeout = OnLockTimeout.STALE)
public Profile load(String id) { ... }
```

| Attribute | Default | Meaning |
|---|---|---|
| `cacheName` | - | Name of the cache. |
| `key` | derived | SpEL: `#name`, `#a0`, `#p0`, `#root.methodName`, `#root.target`. Empty: class, method and all arguments. A failing or null expression raises `KurinCacheKeyException`. |
| `tiers` | configured | Tiers, for a cache that is not configured by properties. |
| `ttl`, `nullTtl` | configured | Lifetime of a value and of a cached null. |
| `jitterPercent` | configured | Each entry lives `ttl ± ttl * percent / 100`, so entries written together do not expire together. |
| `cacheNulls` | `false` | Cache a null result (not for primitive return types). |
| `condition` | - | SpEL on the arguments; false bypasses the cache for the call. |
| `unless` | - | SpEL on `#result`; true returns the result without caching it. |
| `staleWhileRevalidate` | configured | After expiry, return the old value at once for this long while one caller refreshes it. |
| `stampede` | `LOCAL` | `NONE`: every caller loads. `LOCAL`: one load per instance. `CLUSTER`: one load per cluster, given a shared `L3`; needs the locks starter. |
| `lockWait` | `PT5S` | How long a `CLUSTER` read waits for the cluster lock. |
| `onLockTimeout` | `LOAD` | After that wait: `LOAD` from the source anyway, `FAIL` with `KurinCacheLockTimeoutException`, or return a `STALE` value if one is stored. |

Cache plain data, not managed persistence entities.

## `@KurinCacheEvict`

```java
@Transactional
@KurinCacheEvict(cacheNames = "accounts", key = "#id")
public void rename(String id, String name) { ... }

@KurinCacheEvict(cacheNames = {"accounts", "profiles"}, allEntries = true)
public void reimport() { ... }
```

Runs after the method returned successfully; inside a transaction, after it **committed**, and not at all on
rollback. With `replicate = true` (default) the eviction is then applied on every instance. `replicate = false`
keeps it on this instance and the shared store.

This is the intended pattern for an application with its own database: write once, on one instance, in an
ordinary transaction; evict everywhere.

If the cluster is unavailable, the eviction is still done locally and in the shared store, and a warning is
logged: other instances serve their copies until the entries expire.

## `@KurinCachePut`

```java
@KurinCachePut(cacheName = "accounts", key = "#result.id")
public Account create(NewAccount request) { ... }
```

Stores the result on the instance that ran the method. Other instances are not updated; pair it with a ttl.

## On `@KurinCommand` handlers

`@KurinCachePut` and `@KurinCacheEvict` on a command handler are applied on every instance when the command is
applied, with the handler's result as `#result`. Use this only for state that lives in the state machine itself.

## Guarantees and limits

- After a replicated eviction completes on an instance, that instance serves nothing loaded before it. Until
  then (typically milliseconds after the commit) it may serve the old value.
- A load that overlaps an eviction of its key does not populate the cache.
- An instance that catches up from a snapshot, and so cannot know which invalidations it missed, clears its local tiers.
- A cached value is shared by its readers on that instance; do not mutate it.
- Without a `ttl`, an entry missed by an invalidation during a cluster outage stays until the next one.

## Programmatic use

`KurinCacheInvalidator` (`evict`, `evictAll`, returning a future completed when this instance has applied the
invalidation), `KurinCacheManager`, and an `ExternalCacheStore` bean, if present, becomes the `L3` tier.
`KurinCacheObserver` beans are notified of lookups, loads and evictions.

## Startup checks

Validated when the context starts: void methods with `@KurinCacheable` / `@KurinCachePut`, `cacheNulls` on a
primitive return type, missing cache names or keys, unparsable SpEL and durations, jitter outside 0-100.

## Configuration, metrics

`kurin.cache.*`: see the [configuration reference](../README.md#configuration-reference).

| Meter | Type | Tags |
|---|---|---|
| `kurin.cache.gets` | counter | `cache`, `tier` = `L1`/`L2`/`L3`/`none`, `result` = `hit`/`stale`/`miss` |
| `kurin.cache.loads` | counter | `cache`, `result` = `success`/`failure` |
| `kurin.cache.load.duration` | timer | `cache` |
| `kurin.cache.evictions` | counter | `cache`, `reason` |
| `kurin.cache.stampede.waits` | counter | `cache`, `mode` |
| `kurin.cache.l3.circuit.state` | gauge: 0 closed, 1 half-open, 2 open | `cache` |

Observations `kurin.cache.get` and `kurin.cache.load`. Health `kurinCache`, with an
[Actuator adapter](../kurin-actuator-boot3/README.md), is always `UP` and reports `degraded: true` while an external
store circuit is not closed. `/actuator/kurin` gets a `caches` section. With
`kurin.cache.web.trace-header.enabled=true` each HTTP response lists its cache lookups in `X-Kurin-Cache`.
