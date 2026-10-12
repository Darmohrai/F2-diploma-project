package org.kurin.kurincachespringbootstarter;

import org.kurin.kurintieredcache.api.CacheTier;
import org.kurin.kurintieredcache.core.CacheSettings;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "kurin.cache")
public class KurinCacheProperties {
    /**
     * Directory of the disk tiers of this instance. Defaults to the "cache" directory inside the data directory
     * of the Kurin node, which is unique per instance. Two instances must never share it.
     */
    private Path dataDir;
    /**
     * Settings of caches that have no entry under kurin.cache.caches.
     */
    private final Cache defaults = new Cache();
    /**
     * Settings per cache name. Values left out fall back to kurin.cache.defaults.
     */
    private Map<String, Cache> caches = new LinkedHashMap<>();
    private final Codec codec = new Codec();
    private final L2 l2 = new L2();
    private final L3 l3 = new L3();
    private final Invalidation invalidation = new Invalidation();
    private final Web web = new Web();

    public Path getDataDir() { return dataDir; }
    public void setDataDir(Path dataDir) { this.dataDir = dataDir; }

    public Cache getDefaults() { return defaults; }

    public Map<String, Cache> getCaches() { return caches; }
    public void setCaches(Map<String, Cache> caches) { this.caches = caches; }

    public Codec getCodec() { return codec; }

    public L2 getL2() { return l2; }

    public L3 getL3() { return l3; }

    public Invalidation getInvalidation() { return invalidation; }

    public Web getWeb() { return web; }

    /**
     * @return the settings every cache starts from
     */
    public CacheSettings defaultSettings() {
        return defaults.applyTo(CacheSettings.defaults());
    }

    public static class Cache {
        /**
         * Time to live of a cached value. Zero means values never expire. Default 10m.
         */
        private Duration ttl;
        /**
         * Time to live of a cached null. Default: the same as ttl.
         */
        private Duration nullTtl;
        /**
         * Random spread of the time to live in percent (0-100). Default 0.
         */
        private Integer jitterPercent;
        /**
         * How long an expired value is still served while it is refreshed in the background. Default 0 (off).
         */
        private Duration staleWhileRevalidate;
        /**
         * Largest number of entries in the memory tier. Default 10000.
         */
        private Long l1MaxSize;
        /**
         * Tiers of the cache: L1 (memory), L2 (disk), L3 (external store). Default: all that are available.
         */
        private List<CacheTier> tiers;

        public Duration getTtl() { return ttl; }
        public void setTtl(Duration ttl) { this.ttl = ttl; }

        public Duration getNullTtl() { return nullTtl; }
        public void setNullTtl(Duration nullTtl) { this.nullTtl = nullTtl; }

        public Integer getJitterPercent() { return jitterPercent; }
        public void setJitterPercent(Integer jitterPercent) { this.jitterPercent = jitterPercent; }

        public Duration getStaleWhileRevalidate() { return staleWhileRevalidate; }
        public void setStaleWhileRevalidate(Duration staleWhileRevalidate) { this.staleWhileRevalidate = staleWhileRevalidate; }

        public Long getL1MaxSize() { return l1MaxSize; }
        public void setL1MaxSize(Long l1MaxSize) { this.l1MaxSize = l1MaxSize; }

        public List<CacheTier> getTiers() { return tiers; }
        public void setTiers(List<CacheTier> tiers) { this.tiers = tiers; }

        /**
         * @return the base settings with every value set here replaced
         */
        public CacheSettings applyTo(CacheSettings base) {
            CacheSettings settings = base;
            if (ttl != null) {
                settings = settings.withTtl(ttl);
            }
            if (nullTtl != null) {
                settings = settings.withNullTtl(nullTtl);
            }
            if (jitterPercent != null) {
                settings = settings.withJitterPercent(jitterPercent);
            }
            if (staleWhileRevalidate != null) {
                settings = settings.withStaleWhileRevalidate(staleWhileRevalidate);
            }
            if (l1MaxSize != null) {
                settings = settings.withL1MaxSize(l1MaxSize);
            }
            if (tiers != null && !tiers.isEmpty()) {
                settings = settings.withTiers(tiers);
            }
            return settings;
        }
    }

    public static class Codec {
        /**
         * Packages (with subpackages) whose classes may be read back from the disk and external tiers.
         * Required when the external store is shared; empty allows every class.
         */
        private List<String> allowedPackages = new ArrayList<>();

        public List<String> getAllowedPackages() { return allowedPackages; }
        public void setAllowedPackages(List<String> allowedPackages) { this.allowedPackages = allowedPackages; }
    }

    public static class L2 {
        /**
         * Interval of the background sweep that removes expired entries from the disk tier.
         */
        private Duration sweepInterval = Duration.ofMinutes(1);
        /**
         * Largest number of entries one sweep examines per cache, which bounds its disk load.
         */
        private int sweepBatchSize = 1000;

        public Duration getSweepInterval() { return sweepInterval; }
        public void setSweepInterval(Duration sweepInterval) { this.sweepInterval = sweepInterval; }

        public int getSweepBatchSize() { return sweepBatchSize; }
        public void setSweepBatchSize(int sweepBatchSize) { this.sweepBatchSize = sweepBatchSize; }
    }

    public static class L3 {
        /**
         * Longest time one call into the external store may take. A slower call counts as failed.
         */
        private Duration timeout = Duration.ofMillis(100);
        /**
         * Consecutive failures after which the circuit breaker stops calling the external store.
         */
        private int failureThreshold = 5;
        /**
         * How long the circuit stays open before a single probe call is let through.
         */
        private Duration openDuration = Duration.ofSeconds(10);
        /**
         * Number of ordered queues for writes to the external store. Operations on one key keep their order.
         */
        private int writeStripes = 8;

        public Duration getTimeout() { return timeout; }
        public void setTimeout(Duration timeout) { this.timeout = timeout; }

        public int getFailureThreshold() { return failureThreshold; }
        public void setFailureThreshold(int failureThreshold) { this.failureThreshold = failureThreshold; }

        public Duration getOpenDuration() { return openDuration; }
        public void setOpenDuration(Duration openDuration) { this.openDuration = openDuration; }

        public int getWriteStripes() { return writeStripes; }
        public void setWriteStripes(int writeStripes) { this.writeStripes = writeStripes; }
    }

    public static class Invalidation {
        /**
         * Time to wait for the cluster to replicate an invalidation.
         */
        private Duration timeout = Duration.ofSeconds(3);
        /**
         * Number of recently invalidated keys remembered to keep concurrent loads from writing stale values back.
         */
        private long epochCacheSize = 100_000;

        public Duration getTimeout() { return timeout; }
        public void setTimeout(Duration timeout) { this.timeout = timeout; }

        public long getEpochCacheSize() { return epochCacheSize; }
        public void setEpochCacheSize(long epochCacheSize) { this.epochCacheSize = epochCacheSize; }
    }

    public static class Web {
        private final TraceHeader traceHeader = new TraceHeader();

        public TraceHeader getTraceHeader() { return traceHeader; }
    }

    public static class TraceHeader {
        /**
         * Adds an X-Kurin-Cache response header listing the cache lookups of each request. Diagnostic; the
         * response body is buffered while it is on.
         */
        private boolean enabled = false;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }
}
