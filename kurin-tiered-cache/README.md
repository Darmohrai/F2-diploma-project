# kurin-tiered-cache

A cache of up to three tiers per instance, with expiry in every tier and invalidation that can be replicated
through a Kurin node. Plain Java.

| Tier | Where | Survives a restart | Shared between instances |
|---|---|---|---|
| `L1` | memory (Caffeine) | no | no |
| `L2` | local disk (RocksDB) | yes | no |
| `L3` | an `ExternalCacheStore` supplied by the application | depends on the store | if the store says `isShared()` |

```java
KurinCacheManager manager = KurinCacheManager.builder()
        .dataDirectory(Path.of("/var/lib/app/cache"))
        .defaults(defaults)                                   // CacheSettings: ttl, jitter, tiers, sizes
        .allowedPackages(List.of("com.example.model"))        // what may be deserialized from disk and L3
        .externalStore(store)                                  // optional
        .build();
TieredCache accounts = manager.getCache("accounts");
```

Reads go L1, L2, L3 and copy a hit into the faster tiers with its remaining lifetime; `CacheReader` adds
load-on-miss with single flight, stale-while-revalidate and protection against writing back a value that was
invalidated while it was being loaded.

## Guarantees

- An entry is never served after its expiry, in any tier, including after a restart: the expiry travels with
  the entry.
- Writes to a tier are ordered per key; a read after `put` or `evict` on the same instance sees it.
- An eviction that completes during a load prevents that load from populating the cache.
- A slow or failing external store costs at most `externalTimeout` per call until its circuit opens, then nothing;
  the cache keeps working from the local tiers and the source.
- Only classes from the allowed packages (plus JDK value types) are read from disk and from the external store.

## Not guaranteed

- Contents are not replicated. Each instance fills its own L1 and L2.
- `evict` on a manager is local (plus the shared store). To evict on all instances, go through
  `KurinCacheInvalidator` backed by `CacheInvalidationStateMachine` on the node, or the Spring starter.
- With the cluster unavailable a replicated eviction falls back to the local instance and the shared store, and
  the returned future fails so the caller knows other instances may serve the entry until it expires.
- Changing the class of cached values in an incompatible way makes old disk entries unreadable; they are treated
  as misses and removed.

## External store

```java
public interface ExternalCacheStore {
    byte[] get(String key);
    void put(String key, byte[] value, Duration ttl);
    void delete(String key);
    void deleteByPrefix(String prefix);     // "evict all" of one cache
    boolean isShared();                     // true if all instances see the same data
}
```

Implement it over whatever the application already runs. Calls are made from the cache's own threads with a
timeout; the implementation may block. Values are opaque bytes with the expiry inside, so the store's own ttl is
only housekeeping.

Other SPI: `KurinCacheValueCodec` (value format), `KeyGenerator` (default keys), `KurinCacheObserver` (lookups,
loads, evictions, stampede waits, circuit state), `CacheClusterLock` (the lock behind `StampedeMode.CLUSTER`).
