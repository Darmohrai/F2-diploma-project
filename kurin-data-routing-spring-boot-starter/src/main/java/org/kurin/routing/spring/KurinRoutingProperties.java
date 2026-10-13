package org.kurin.routing.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "kurin.routing")
public class KurinRoutingProperties {

    public enum ConsistencyStrategy {
        /**
         * Reads go to the primary for sticky-window after a write. Works across instances, needs no database support.
         */
        TIMESTAMP,
        /**
         * Reads go to the primary until the replica has replayed the write. Needs a ReplicaPositionProbe.
         */
        REPLICA_POSITION,
        /**
         * Stickiness is remembered in the memory of the instance that handled the write. For single-instance setups.
         */
        LOCAL_SESSION
    }

    public enum ReplicaFallback {
        /**
         * Reads of a shard whose replica is down go to its primary.
         */
        PRIMARY,
        /**
         * Reads of a shard whose replica is down fail.
         */
        FAIL
    }

    /**
     * Treat @Transactional(readOnly = true) like @KurinReadOnly: such transactions may run on a replica.
     */
    private boolean readonlyTransactionsToReplica = false;
    /**
     * How read-your-writes is decided for READ_YOUR_WRITES reads.
     */
    private ConsistencyStrategy consistencyStrategy = ConsistencyStrategy.TIMESTAMP;
    /**
     * For the TIMESTAMP and LOCAL_SESSION strategies: how long after a write the writer reads from the primary.
     * Set it above the replication lag you expect.
     */
    private Duration stickyWindow = Duration.ofSeconds(3);
    /**
     * For the REPLICA_POSITION strategy: how long a replica position read from the database is reused.
     */
    private Duration probeCache = Duration.ofMillis(200);
    /**
     * Key that signs consistency tokens, the same on all instances. Without it tokens are not signed.
     */
    private String tokenSecret;
    /**
     * What a read does when the replica of its shard is down.
     */
    private ReplicaFallback replicaFallback = ReplicaFallback.PRIMARY;
    /**
     * Points on the consistent hash ring for a shard of weight 1.
     */
    private int virtualNodes = 160;
    /**
     * Per shard settings, by shard name. Overrides the weight given in the topology.
     */
    private Map<String, ShardProperties> shards = new LinkedHashMap<>();
    private final Token token = new Token();
    private final Health health = new Health();
    private final FanOut fanOut = new FanOut();
    private final PostgresProbe postgresProbe = new PostgresProbe();

    public boolean isReadonlyTransactionsToReplica() { return readonlyTransactionsToReplica; }
    public void setReadonlyTransactionsToReplica(boolean readonlyTransactionsToReplica) { this.readonlyTransactionsToReplica = readonlyTransactionsToReplica; }

    public ConsistencyStrategy getConsistencyStrategy() { return consistencyStrategy; }
    public void setConsistencyStrategy(ConsistencyStrategy consistencyStrategy) { this.consistencyStrategy = consistencyStrategy; }

    public Duration getStickyWindow() { return stickyWindow; }
    public void setStickyWindow(Duration stickyWindow) { this.stickyWindow = stickyWindow; }

    public Duration getProbeCache() { return probeCache; }
    public void setProbeCache(Duration probeCache) { this.probeCache = probeCache; }

    public String getTokenSecret() { return tokenSecret; }
    public void setTokenSecret(String tokenSecret) { this.tokenSecret = tokenSecret; }

    public ReplicaFallback getReplicaFallback() { return replicaFallback; }
    public void setReplicaFallback(ReplicaFallback replicaFallback) { this.replicaFallback = replicaFallback; }

    public int getVirtualNodes() { return virtualNodes; }
    public void setVirtualNodes(int virtualNodes) { this.virtualNodes = virtualNodes; }

    public Map<String, ShardProperties> getShards() { return shards; }
    public void setShards(Map<String, ShardProperties> shards) { this.shards = shards; }

    public Token getToken() { return token; }

    public Health getHealth() { return health; }

    public FanOut getFanOut() { return fanOut; }

    public PostgresProbe getPostgresProbe() { return postgresProbe; }

    public static class ShardProperties {
        /**
         * Relative share of the keys this shard owns. A shard of weight 2 gets about twice the keys of weight 1.
         */
        private Integer weight;

        public Integer getWeight() { return weight; }
        public void setWeight(Integer weight) { this.weight = weight; }
    }

    public static class Token {
        /**
         * Request and response header that carries the consistency token. Empty disables the header.
         */
        private String headerName = "X-Kurin-Consistency";
        /**
         * Cookie that carries the consistency token (HttpOnly, SameSite=Lax). Empty disables the cookie.
         */
        private String cookieName = "";

        public String getHeaderName() { return headerName; }
        public void setHeaderName(String headerName) { this.headerName = headerName; }

        public String getCookieName() { return cookieName; }
        public void setCookieName(String cookieName) { this.cookieName = cookieName; }
    }

    public static class Health {
        /**
         * Check the database nodes periodically.
         */
        private boolean enabled = true;
        /**
         * Time between checks.
         */
        private Duration interval = Duration.ofSeconds(5);
        /**
         * Longest time one check may take.
         */
        private Duration timeout = Duration.ofSeconds(1);
        /**
         * Successful checks in a row after which a node that was down is used again.
         */
        private int recoveryThreshold = 2;
        /**
         * Report the application as out of service when a primary is down.
         */
        private boolean primaryRequired = false;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }

        public Duration getInterval() { return interval; }
        public void setInterval(Duration interval) { this.interval = interval; }

        public Duration getTimeout() { return timeout; }
        public void setTimeout(Duration timeout) { this.timeout = timeout; }

        public int getRecoveryThreshold() { return recoveryThreshold; }
        public void setRecoveryThreshold(int recoveryThreshold) { this.recoveryThreshold = recoveryThreshold; }

        public boolean isPrimaryRequired() { return primaryRequired; }
        public void setPrimaryRequired(boolean primaryRequired) { this.primaryRequired = primaryRequired; }
    }

    public static class FanOut {
        /**
         * Largest number of shards worked on at the same time by executeOnAllShards, across all callers.
         */
        private int parallelism = 8;

        public int getParallelism() { return parallelism; }
        public void setParallelism(int parallelism) { this.parallelism = parallelism; }
    }

    public static class PostgresProbe {
        /**
         * Use the built-in PostgreSQL probe (pg_current_wal_lsn / pg_last_wal_replay_lsn) for the
         * REPLICA_POSITION strategy.
         */
        private boolean enabled = false;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }
}
