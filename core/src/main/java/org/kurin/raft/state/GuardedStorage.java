package org.kurin.raft.state;

import org.kurin.api.exception.KurinStorageException;
import org.kurin.network.model.NodeAddress;
import org.kurin.raft.log.LogEntry;
import org.kurin.raft.log.RaftLog;
import org.kurin.raft.snapshot.Snapshot;
import org.kurin.raft.snapshot.SnapshotStore;

import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Wraps the storage of a node so that an I/O failure of any operation is reported to the Raft state before it
 * reaches the caller. The caller sees a {@link KurinStorageException}; the Raft state takes the node out of the
 * cluster when the running task ends. Failures that are not I/O errors (a caller breaking the contract of the
 * log, for instance) pass through unchanged.
 */
final class GuardedStorage {
    private final Function<UncheckedIOException, KurinStorageException> onFailure;

    GuardedStorage(Function<UncheckedIOException, KurinStorageException> onFailure) {
        this.onFailure = onFailure;
    }

    private void run(Runnable operation) {
        try {
            operation.run();
        } catch (UncheckedIOException e) {
            throw onFailure.apply(e);
        }
    }

    private <T> T call(Supplier<T> operation) {
        try {
            return operation.get();
        } catch (UncheckedIOException e) {
            throw onFailure.apply(e);
        }
    }

    RaftLog guard(RaftLog log) {
        return new RaftLog() {
            @Override
            public void append(LogEntry entry) {
                run(() -> log.append(entry));
            }

            @Override
            public void append(List<LogEntry> entries) {
                run(() -> log.append(entries));
            }

            @Override
            public LogEntry getEntry(long index) {
                return call(() -> log.getEntry(index));
            }

            @Override
            public List<LogEntry> getEntriesFrom(long index) {
                return call(() -> log.getEntriesFrom(index));
            }

            @Override
            public long getLastLogIndex() {
                return log.getLastLogIndex();
            }

            @Override
            public long getLastLogTerm() {
                return log.getLastLogTerm();
            }

            @Override
            public void truncateFrom(long index) {
                run(() -> log.truncateFrom(index));
            }

            @Override
            public long getBaseIndex() {
                return log.getBaseIndex();
            }

            @Override
            public long getBaseTerm() {
                return log.getBaseTerm();
            }

            @Override
            public void compact(long upToIndex, long upToTerm) {
                run(() -> log.compact(upToIndex, upToTerm));
            }

            @Override
            public void reset(long baseIndex, long baseTerm) {
                run(() -> log.reset(baseIndex, baseTerm));
            }

            @Override
            public void sync() {
                run(log::sync);
            }

            @Override
            public void recover() {
                run(log::recover);
            }

            @Override
            public long getDurableIndex() {
                return log.getDurableIndex();
            }

            @Override
            public void close() {
                log.close();
            }
        };
    }

    SnapshotStore guard(SnapshotStore store) {
        return new SnapshotStore() {
            @Override
            public Snapshot load() {
                return call(store::load);
            }

            @Override
            public void save(Snapshot snapshot) {
                run(() -> store.save(snapshot));
            }

            @Override
            public void verifyWritable() {
                run(store::verifyWritable);
            }
        };
    }

    PersistentMetadata guard(PersistentMetadata metadata) {
        return new PersistentMetadata() {
            @Override
            public long getCurrentTerm() {
                return metadata.getCurrentTerm();
            }

            @Override
            public NodeAddress getVotedFor() {
                return metadata.getVotedFor();
            }

            @Override
            public void save(long term, NodeAddress votedFor) {
                run(() -> metadata.save(term, votedFor));
            }
        };
    }
}
