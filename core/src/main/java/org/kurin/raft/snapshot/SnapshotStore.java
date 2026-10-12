package org.kurin.raft.snapshot;

/**
 * Keeps the latest snapshot of a node.
 */
public interface SnapshotStore {

    /**
     * @return the latest snapshot, or null if none was ever saved
     */
    Snapshot load();

    /**
     * Replaces the stored snapshot. The new one is durable before the method returns.
     */
    void save(Snapshot snapshot);
}
