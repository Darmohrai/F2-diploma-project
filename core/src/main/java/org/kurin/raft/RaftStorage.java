package org.kurin.raft;

import org.kurin.raft.log.RaftLog;
import org.kurin.raft.snapshot.SnapshotStore;
import org.kurin.raft.state.PersistentMetadata;

/**
 * The durable state of one node: its log, its latest snapshot, its term and vote. All three are used from the
 * Raft thread only. An implementation reports an I/O failure with {@link java.io.UncheckedIOException}; the node
 * then stops taking part in the cluster until {@link RaftLog#recover()}, {@link PersistentMetadata#save} and
 * {@link SnapshotStore#verifyWritable()} succeed again.
 */
public record RaftStorage(RaftLog log, SnapshotStore snapshots, PersistentMetadata metadata) {
}
