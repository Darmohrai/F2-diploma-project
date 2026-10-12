package org.kurin.raft.snapshot;

public class InMemorySnapshotStore implements SnapshotStore {
    private Snapshot snapshot;

    @Override
    public Snapshot load() {
        return snapshot;
    }

    @Override
    public void save(Snapshot snapshot) {
        this.snapshot = snapshot;
    }
}
