package org.kurin.kurintieredcache.core;

import org.kurin.kurintieredcache.api.CacheTier;
import org.kurin.kurintieredcache.api.ExternalCacheStore;
import org.kurin.kurintieredcache.api.KurinCacheException;
import org.kurin.kurintieredcache.api.KurinCacheObserver;
import org.kurin.kurintieredcache.api.KurinCacheValueCodec;
import org.kurin.kurintieredcache.codec.KryoCacheCodec;
import org.kurin.kurintieredcache.tier.CaffeineTier;
import org.kurin.kurintieredcache.tier.ExternalTier;
import org.kurin.kurintieredcache.tier.RocksDbTier;
import org.kurin.kurintieredcache.tier.StripedExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Creates and owns the caches of one application instance.
 * <p>
 * A cache gets the tiers named by its settings that are actually available: the disk tier needs a data
 * directory, the external tier needs an {@link ExternalCacheStore}.
 */
public class KurinCacheManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KurinCacheManager.class);
    private static final String SEQUENCE_FILE = "invalidation.seq";
    private static final String DISK_DIRECTORY = "l2";

    private final CacheSettings defaults;
    private final Map<String, CacheSettings> perCacheSettings;
    private final Path dataDirectory;
    private final KurinCacheValueCodec codec;
    private final ExternalCacheStore externalStore;
    private final Duration externalTimeout;
    private final int externalFailureThreshold;
    private final Duration externalOpenDuration;
    private final int sweepBatchSize;

    private final InvalidationEpochs epochs;
    private final ObserverHub observers = new ObserverHub();
    private final Map<String, TieredCache> caches = new ConcurrentHashMap<>();
    private final ExternalTier.Resources externalResources;
    private final ScheduledExecutorService sweeper;
    private volatile boolean closed;

    private KurinCacheManager(Builder builder) {
        this.defaults = builder.defaults;
        this.perCacheSettings = Map.copyOf(builder.perCacheSettings);
        this.dataDirectory = builder.dataDirectory;
        this.codec = builder.codec != null ? builder.codec : new KryoCacheCodec(builder.allowedPackages);
        this.externalStore = builder.externalStore;
        this.externalTimeout = builder.externalTimeout;
        this.externalFailureThreshold = builder.externalFailureThreshold;
        this.externalOpenDuration = builder.externalOpenDuration;
        this.sweepBatchSize = builder.sweepBatchSize;
        this.epochs = new InvalidationEpochs(builder.epochCacheSize);
        builder.observers.forEach(observers::add);

        if (externalStore != null && externalStore.isShared() && this.codec instanceof KryoCacheCodec kryo && !kryo.restrictsTypes()) {
            throw new KurinCacheException("A shared external cache store is configured but no allowed packages are set. "
                    + "Other parties can write to a shared store, so the classes that may be read back from it must be "
                    + "listed in kurin.cache.codec.allowed-packages.");
        }

        if (externalStore != null) {
            AtomicInteger threadNumber = new AtomicInteger();
            ExecutorService ioExecutor = Executors.newCachedThreadPool(task -> {
                Thread thread = new Thread(task, "kurin-cache-l3-io-" + threadNumber.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            });
            this.externalResources = new ExternalTier.Resources(ioExecutor, new StripedExecutor(builder.externalWriteStripes, "kurin-cache-l3-write"));
        } else {
            this.externalResources = null;
        }

        if (dataDirectory != null) {
            try {
                Files.createDirectories(dataDirectory.resolve(DISK_DIRECTORY));
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot create the cache data directory " + dataDirectory, e);
            }
            if (builder.clearDiskOnStart) {
                // Nothing replays the invalidations this instance missed while it was down
                deleteDiskDirectories(List.of());
                recordInvalidationSequence(0);
            }
            this.sweeper = Executors.newSingleThreadScheduledExecutor(task -> {
                Thread thread = new Thread(task, "kurin-cache-sweeper");
                thread.setDaemon(true);
                return thread;
            });
            long intervalMs = Math.max(1, builder.sweepInterval.toMillis());
            sweeper.scheduleWithFixedDelay(this::sweep, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        } else {
            this.sweeper = null;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    // ---------------------------------------------------------------- caches

    /**
     * @return the cache with the given name, created from its settings on first use
     */
    public TieredCache getCache(String name) {
        return getCache(name, List.of());
    }

    /**
     * @param requestedTiers tiers asked for by the caller, used if the cache is created now and has no
     *                       configured settings of its own; empty means no preference
     * @throws KurinCacheException if the cache already exists with other tiers than the caller asks for
     */
    public TieredCache getCache(String name, List<CacheTier> requestedTiers) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Cache name must not be blank");
        }
        TieredCache cache = caches.get(name);
        if (cache == null) {
            cache = caches.computeIfAbsent(name, cacheName -> create(cacheName, requestedTiers));
        }
        if (!requestedTiers.isEmpty() && !perCacheSettings.containsKey(name)) {
            List<CacheTier> available = availableTiers(name, requestedTiers);
            if (!cache.tiers().equals(available)) {
                throw new KurinCacheException("Cache '" + name + "' already exists with tiers " + cache.tiers()
                        + " but is requested with " + available + ". Declare the tiers of a cache once, in "
                        + "kurin.cache.caches." + name + ".tiers or consistently on its annotations.");
            }
        }
        return cache;
    }

    public CacheSettings settingsOf(String name) {
        return perCacheSettings.getOrDefault(name, defaults);
    }

    public Collection<TieredCache> caches() {
        return List.copyOf(caches.values());
    }

    private TieredCache create(String name, List<CacheTier> requestedTiers) {
        if (closed) {
            throw new KurinCacheException("The cache manager is closed");
        }
        CacheSettings settings = settingsOf(name);
        if (!perCacheSettings.containsKey(name) && !requestedTiers.isEmpty()) {
            settings = settings.withTiers(requestedTiers);
        }
        List<CacheTier> tiers = availableTiers(name, settings.tiers());

        CaffeineTier memory = tiers.contains(CacheTier.L1) ? new CaffeineTier(name, settings.l1MaxSize()) : null;
        RocksDbTier disk = tiers.contains(CacheTier.L2) ? new RocksDbTier(name, diskDirectoryOf(name), codec) : null;
        ExternalTier external = tiers.contains(CacheTier.L3)
                ? new ExternalTier(name, externalStore, codec, externalResources, externalTimeout, externalFailureThreshold,
                externalOpenDuration, state -> observers.onExternalStoreCircuit(name, state.name()))
                : null;
        log.info("Created cache '{}' with tiers {}", name, tiers);
        return new TieredCache(name, settings.withTiers(tiers), memory, disk, external, epochs, observers);
    }

    private List<CacheTier> availableTiers(String name, List<CacheTier> wanted) {
        List<CacheTier> tiers = new ArrayList<>();
        for (CacheTier tier : wanted.stream().distinct().sorted().toList()) {
            if (tier == CacheTier.L2 && dataDirectory == null || tier == CacheTier.L3 && externalStore == null) {
                continue;
            }
            tiers.add(tier);
        }
        if (tiers.isEmpty()) {
            throw new KurinCacheException("Cache '" + name + "' asks for tiers " + wanted + ", none of which is available: "
                    + "L2 needs a cache data directory and L3 needs an ExternalCacheStore");
        }
        return tiers;
    }

    private Path diskDirectoryOf(String cacheName) {
        // The cache name becomes a directory name
        return dataDirectory.resolve(DISK_DIRECTORY).resolve(cacheName.replaceAll("[^A-Za-z0-9._-]", "_"));
    }

    public InvalidationEpochs epochs() {
        return epochs;
    }

    public ObserverHub observers() {
        return observers;
    }

    public void addObserver(KurinCacheObserver observer) {
        observers.add(observer);
    }

    public Duration externalTimeout() {
        return externalTimeout;
    }

    // ---------------------------------------------------------------- local state of replicated invalidation

    /**
     * Drops everything this instance holds locally, including disk tiers of caches not opened yet.
     */
    public void clearAllLocal(String reason) {
        List<String> opened = new ArrayList<>();
        for (TieredCache cache : caches.values()) {
            cache.clearLocal(reason);
            opened.add(diskDirectoryOf(cache.name()).getFileName().toString());
        }
        if (dataDirectory != null) {
            deleteDiskDirectories(opened);
        }
    }

    private void deleteDiskDirectories(List<String> keep) {
        Path root = dataDirectory.resolve(DISK_DIRECTORY);
        try (Stream<Path> directories = Files.list(root)) {
            for (Path directory : directories.filter(Files::isDirectory).toList()) {
                if (keep.contains(directory.getFileName().toString())) {
                    continue;
                }
                try (Stream<Path> content = Files.walk(directory)) {
                    for (Path path : content.sorted(Comparator.reverseOrder()).toList()) {
                        Files.delete(path);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot clear the disk tiers in " + root, e);
        }
    }

    /**
     * @return the number of replicated invalidations whose effect is reflected in the local tiers of this
     * instance; 0 if unknown
     */
    public long localInvalidationSequence() {
        if (dataDirectory == null) {
            return 0;
        }
        Path file = dataDirectory.resolve(SEQUENCE_FILE);
        try {
            return Files.exists(file) ? Long.parseLong(Files.readString(file, StandardCharsets.UTF_8).trim()) : 0;
        } catch (IOException | NumberFormatException e) {
            log.warn("Cannot read {}: {}. Local cache tiers are treated as out of date.", file, e.toString());
            return -1;
        }
    }

    public void recordInvalidationSequence(long sequence) {
        if (dataDirectory == null) {
            return;
        }
        try {
            Files.writeString(dataDirectory.resolve(SEQUENCE_FILE), Long.toString(sequence), StandardCharsets.UTF_8);
        } catch (IOException e) {
            // On the next restart the mismatch makes this instance clear its disk tiers, which is safe
            log.warn("Cannot record the invalidation sequence in {}: {}", dataDirectory, e.toString());
        }
    }

    private void sweep() {
        for (TieredCache cache : caches.values()) {
            try {
                int removed = cache.sweepDisk(sweepBatchSize);
                if (removed > 0) {
                    log.debug("Swept {} expired entries from the disk tier of cache '{}'", removed, cache.name());
                }
            } catch (RuntimeException e) {
                log.warn("Sweep of cache '{}' failed: {}", cache.name(), e.toString());
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        if (sweeper != null) {
            sweeper.shutdownNow();
            try {
                // A sweep in progress must leave the databases before they are closed
                sweeper.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (externalResources != null) {
            externalResources.writes().close();
            externalResources.ioExecutor().shutdown();
        }
        for (TieredCache cache : caches.values()) {
            try {
                cache.close();
            } catch (RuntimeException e) {
                log.error("Error closing cache '{}'", cache.name(), e);
            }
        }
        caches.clear();
    }

    public static final class Builder {
        private CacheSettings defaults = CacheSettings.defaults();
        private final Map<String, CacheSettings> perCacheSettings = new HashMap<>();
        private Path dataDirectory;
        private KurinCacheValueCodec codec;
        private List<String> allowedPackages = List.of();
        private ExternalCacheStore externalStore;
        private Duration externalTimeout = Duration.ofMillis(100);
        private int externalFailureThreshold = 5;
        private Duration externalOpenDuration = Duration.ofSeconds(10);
        private int externalWriteStripes = 8;
        private Duration sweepInterval = Duration.ofMinutes(1);
        private int sweepBatchSize = 1000;
        private long epochCacheSize = 100_000;
        private boolean clearDiskOnStart;
        private final List<KurinCacheObserver> observers = new ArrayList<>();

        public Builder defaults(CacheSettings defaults) {
            this.defaults = defaults;
            return this;
        }

        public Builder cache(String name, CacheSettings settings) {
            perCacheSettings.put(name, settings);
            return this;
        }

        /**
         * @param dataDirectory directory of this instance for disk tiers; null disables the disk tier.
         *                      Two instances must never share it.
         */
        public Builder dataDirectory(Path dataDirectory) {
            this.dataDirectory = dataDirectory;
            return this;
        }

        public Builder codec(KurinCacheValueCodec codec) {
            this.codec = codec;
            return this;
        }

        /**
         * Packages whose classes the default codec may read back from stored bytes.
         */
        public Builder allowedPackages(List<String> allowedPackages) {
            this.allowedPackages = List.copyOf(allowedPackages);
            return this;
        }

        public Builder externalStore(ExternalCacheStore externalStore) {
            this.externalStore = externalStore;
            return this;
        }

        public Builder externalTimeout(Duration externalTimeout) {
            this.externalTimeout = externalTimeout;
            return this;
        }

        public Builder externalCircuit(int failureThreshold, Duration openDuration) {
            this.externalFailureThreshold = failureThreshold;
            this.externalOpenDuration = openDuration;
            return this;
        }

        public Builder externalWriteStripes(int stripes) {
            this.externalWriteStripes = stripes;
            return this;
        }

        public Builder sweep(Duration interval, int batchSize) {
            this.sweepInterval = interval;
            this.sweepBatchSize = batchSize;
            return this;
        }

        public Builder epochCacheSize(long epochCacheSize) {
            this.epochCacheSize = epochCacheSize;
            return this;
        }

        /**
         * Empty the disk tiers when the manager is created. Needed when invalidations are not replayed after a
         * restart, for example when the Raft log is kept in memory only.
         */
        public Builder clearDiskOnStart(boolean clearDiskOnStart) {
            this.clearDiskOnStart = clearDiskOnStart;
            return this;
        }

        public Builder observer(KurinCacheObserver observer) {
            observers.add(observer);
            return this;
        }

        public KurinCacheManager build() {
            return new KurinCacheManager(this);
        }
    }
}
