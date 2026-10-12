package org.kurin.kurinspringbootstarter.starter;

import org.kurin.raft.RaftConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "kurin.node")
public class KurinProperties {
    /**
     * Name shared by all nodes of one cluster. Nodes with different names refuse to connect.
     */
    private String clusterName = "kurin";
    /**
     * Host other nodes use to reach this node; with the port it forms the node id. Required unless every
     * peer is a loopback address.
     */
    private String advertisedHost;
    /**
     * Local interface the node listens on.
     */
    private String bindHost = "0.0.0.0";
    /**
     * Port of the node-to-node protocol.
     */
    private int port;
    /**
     * Cluster members as host:port. May include this node, so all nodes can share one list.
     */
    private List<String> peers = new ArrayList<>();
    /**
     * Directory of the durable node state. Defaults to ./kurin-data/<advertised-host>-<port>.
     */
    private Path dataDir;
    /**
     * FILE keeps term, vote, log and snapshots on disk. MEMORY keeps nothing across restarts.
     */
    private RaftConfig.StorageType storage = RaftConfig.StorageType.FILE;
    /**
     * Interval between leader heartbeats.
     */
    private Duration heartbeatInterval = Duration.ofMillis(100);
    /**
     * Lower bound of the randomized election timeout. Must be at least five heartbeat intervals.
     */
    private Duration electionTimeoutMin = Duration.ofSeconds(1);
    /**
     * Upper bound of the randomized election timeout.
     */
    private Duration electionTimeoutMax = Duration.ofSeconds(2);
    /**
     * Fraction of election-timeout-min for which a leader may trust its lease without contacting a majority.
     */
    private double leaseRatio = 0.8;
    /**
     * Default time to wait for the result of a command or query.
     */
    private Duration commandTimeout = Duration.ofSeconds(5);
    /**
     * Largest message accepted on the wire.
     */
    private DataSize maxPayloadSize = DataSize.ofMegabytes(1);
    /**
     * Largest number of log entries in one replication request.
     */
    private int maxEntriesPerAppend = 512;
    /**
     * Number of replication requests that may be in flight to one follower.
     */
    private int maxInflightAppends = 1;
    /**
     * Number of applied log entries after which a snapshot is taken and the log is compacted.
     */
    private long snapshotThreshold = 10_000;
    /**
     * Number of request ids remembered for deduplication of retried commands.
     */
    private int dedupCacheSize = 10_000;
    /**
     * Shared secret. When set, nodes authenticate each other with an HMAC during the handshake.
     */
    private String clusterSecret;
    /**
     * Idle time after which a node connection is closed.
     */
    private Duration idleTimeout = Duration.ofSeconds(30);
    private final Tls tls = new Tls();

    public String getClusterName() { return clusterName; }
    public void setClusterName(String clusterName) { this.clusterName = clusterName; }

    public String getAdvertisedHost() { return advertisedHost; }
    public void setAdvertisedHost(String advertisedHost) { this.advertisedHost = advertisedHost; }

    public String getBindHost() { return bindHost; }
    public void setBindHost(String bindHost) { this.bindHost = bindHost; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public List<String> getPeers() { return peers; }
    public void setPeers(List<String> peers) { this.peers = peers; }

    public Path getDataDir() { return dataDir; }
    public void setDataDir(Path dataDir) { this.dataDir = dataDir; }

    public RaftConfig.StorageType getStorage() { return storage; }
    public void setStorage(RaftConfig.StorageType storage) { this.storage = storage; }

    public Duration getHeartbeatInterval() { return heartbeatInterval; }
    public void setHeartbeatInterval(Duration heartbeatInterval) { this.heartbeatInterval = heartbeatInterval; }

    public Duration getElectionTimeoutMin() { return electionTimeoutMin; }
    public void setElectionTimeoutMin(Duration electionTimeoutMin) { this.electionTimeoutMin = electionTimeoutMin; }

    public Duration getElectionTimeoutMax() { return electionTimeoutMax; }
    public void setElectionTimeoutMax(Duration electionTimeoutMax) { this.electionTimeoutMax = electionTimeoutMax; }

    public double getLeaseRatio() { return leaseRatio; }
    public void setLeaseRatio(double leaseRatio) { this.leaseRatio = leaseRatio; }

    public Duration getCommandTimeout() { return commandTimeout; }
    public void setCommandTimeout(Duration commandTimeout) { this.commandTimeout = commandTimeout; }

    public DataSize getMaxPayloadSize() { return maxPayloadSize; }
    public void setMaxPayloadSize(DataSize maxPayloadSize) { this.maxPayloadSize = maxPayloadSize; }

    public int getMaxEntriesPerAppend() { return maxEntriesPerAppend; }
    public void setMaxEntriesPerAppend(int maxEntriesPerAppend) { this.maxEntriesPerAppend = maxEntriesPerAppend; }

    public int getMaxInflightAppends() { return maxInflightAppends; }
    public void setMaxInflightAppends(int maxInflightAppends) { this.maxInflightAppends = maxInflightAppends; }

    public long getSnapshotThreshold() { return snapshotThreshold; }
    public void setSnapshotThreshold(long snapshotThreshold) { this.snapshotThreshold = snapshotThreshold; }

    public int getDedupCacheSize() { return dedupCacheSize; }
    public void setDedupCacheSize(int dedupCacheSize) { this.dedupCacheSize = dedupCacheSize; }

    public String getClusterSecret() { return clusterSecret; }
    public void setClusterSecret(String clusterSecret) { this.clusterSecret = clusterSecret; }

    public Duration getIdleTimeout() { return idleTimeout; }
    public void setIdleTimeout(Duration idleTimeout) { this.idleTimeout = idleTimeout; }

    public Tls getTls() { return tls; }

    public static class Tls {
        /**
         * Encrypts node connections with mutual TLS.
         */
        private boolean enabled;
        /**
         * Key store with the certificate and private key of this node.
         */
        private String keyStore;
        private String keyStorePassword;
        private String keyStoreType = "PKCS12";
        /**
         * Trust store with the certificates of the other nodes or of their CA.
         */
        private String trustStore;
        private String trustStorePassword;
        private String trustStoreType = "PKCS12";

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }

        public String getKeyStore() { return keyStore; }
        public void setKeyStore(String keyStore) { this.keyStore = keyStore; }

        public String getKeyStorePassword() { return keyStorePassword; }
        public void setKeyStorePassword(String keyStorePassword) { this.keyStorePassword = keyStorePassword; }

        public String getKeyStoreType() { return keyStoreType; }
        public void setKeyStoreType(String keyStoreType) { this.keyStoreType = keyStoreType; }

        public String getTrustStore() { return trustStore; }
        public void setTrustStore(String trustStore) { this.trustStore = trustStore; }

        public String getTrustStorePassword() { return trustStorePassword; }
        public void setTrustStorePassword(String trustStorePassword) { this.trustStorePassword = trustStorePassword; }

        public String getTrustStoreType() { return trustStoreType; }
        public void setTrustStoreType(String trustStoreType) { this.trustStoreType = trustStoreType; }
    }
}
