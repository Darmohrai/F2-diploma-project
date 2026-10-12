package org.kurin.raft;

import java.nio.file.Path;

public class RaftConfig {

    public enum StorageType {
        /**
         * Nothing survives a restart. For tests and for nodes that are always re-seeded from peers.
         */
        MEMORY,
        FILE
    }

    private long heartbeatIntervalMs = 100;
    private long electionTimeoutMinMs = 1000;
    private long electionTimeoutMaxMs = 2000;
    private double leaseRatio = 0.8;
    private long commandTimeoutMs = 5000;
    private int maxEntriesPerAppend = 512;
    private int maxInflightAppends = 1;
    private int maxAppendBytes = 0;
    private long snapshotThresholdEntries = 10_000;
    private int dedupCacheSize = 10_000;
    private StorageType storage = StorageType.FILE;
    private Path dataDir;

    /**
     * @throws IllegalArgumentException if the timing settings cannot produce a stable cluster
     */
    public void validate() {
        if (heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("heartbeatIntervalMs must be positive");
        }
        if (electionTimeoutMinMs < 5 * heartbeatIntervalMs) {
            throw new IllegalArgumentException("electionTimeoutMinMs (" + electionTimeoutMinMs
                    + ") must be at least 5 * heartbeatIntervalMs (" + heartbeatIntervalMs + ")");
        }
        if (electionTimeoutMaxMs <= electionTimeoutMinMs) {
            throw new IllegalArgumentException("electionTimeoutMaxMs (" + electionTimeoutMaxMs
                    + ") must be greater than electionTimeoutMinMs (" + electionTimeoutMinMs + ")");
        }
        if (leaseRatio <= 0 || leaseRatio >= 1) {
            throw new IllegalArgumentException("leaseRatio must be between 0 and 1 (exclusive)");
        }
        if (commandTimeoutMs <= 0 || maxEntriesPerAppend <= 0 || maxInflightAppends <= 0
                || snapshotThresholdEntries <= 0 || dedupCacheSize <= 0) {
            throw new IllegalArgumentException("commandTimeoutMs, maxEntriesPerAppend, maxInflightAppends, "
                    + "snapshotThresholdEntries and dedupCacheSize must be positive");
        }
    }

    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public long getElectionTimeoutMinMs() {
        return electionTimeoutMinMs;
    }

    public void setElectionTimeoutMinMs(long electionTimeoutMinMs) {
        this.electionTimeoutMinMs = electionTimeoutMinMs;
    }

    public long getElectionTimeoutMaxMs() {
        return electionTimeoutMaxMs;
    }

    public void setElectionTimeoutMaxMs(long electionTimeoutMaxMs) {
        this.electionTimeoutMaxMs = electionTimeoutMaxMs;
    }

    public double getLeaseRatio() {
        return leaseRatio;
    }

    public void setLeaseRatio(double leaseRatio) {
        this.leaseRatio = leaseRatio;
    }

    public long getCommandTimeoutMs() {
        return commandTimeoutMs;
    }

    public void setCommandTimeoutMs(long commandTimeoutMs) {
        this.commandTimeoutMs = commandTimeoutMs;
    }

    public int getMaxEntriesPerAppend() {
        return maxEntriesPerAppend;
    }

    public void setMaxEntriesPerAppend(int maxEntriesPerAppend) {
        this.maxEntriesPerAppend = maxEntriesPerAppend;
    }

    public int getMaxInflightAppends() {
        return maxInflightAppends;
    }

    public void setMaxInflightAppends(int maxInflightAppends) {
        this.maxInflightAppends = maxInflightAppends;
    }

    /**
     * @return approximate byte budget of one AppendEntries batch; 0 means half of the network payload limit
     */
    public int getMaxAppendBytes() {
        return maxAppendBytes;
    }

    public void setMaxAppendBytes(int maxAppendBytes) {
        this.maxAppendBytes = maxAppendBytes;
    }

    public long getSnapshotThresholdEntries() {
        return snapshotThresholdEntries;
    }

    public void setSnapshotThresholdEntries(long snapshotThresholdEntries) {
        this.snapshotThresholdEntries = snapshotThresholdEntries;
    }

    public int getDedupCacheSize() {
        return dedupCacheSize;
    }

    public void setDedupCacheSize(int dedupCacheSize) {
        this.dedupCacheSize = dedupCacheSize;
    }

    public StorageType getStorage() {
        return storage;
    }

    public void setStorage(StorageType storage) {
        this.storage = storage;
    }

    /**
     * @return directory of this node's durable state; null means {@code ./kurin-data/<host>-<port>}
     */
    public Path getDataDir() {
        return dataDir;
    }

    public void setDataDir(Path dataDir) {
        this.dataDir = dataDir;
    }
}
