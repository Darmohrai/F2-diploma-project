package org.kurin.test;

import org.kurin.network.model.NodeAddress;
import org.kurin.raft.RaftStorage;
import org.kurin.raft.log.LogEntry;
import org.kurin.raft.log.RaftLog;
import org.kurin.raft.snapshot.Snapshot;
import org.kurin.raft.snapshot.SnapshotStore;
import org.kurin.raft.state.PersistentMetadata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Makes the storage of a node fail on demand, the way a full or dying disk does: every write is refused with an
 * I/O error and leaves the real storage untouched, reads of what is held in memory keep working. One instance
 * belongs to one node and survives its restarts.
 */
public final class FaultInjectingStorage {
    private volatile boolean logBroken;
    private volatile boolean snapshotsBroken;
    private volatile boolean metadataBroken;
    private final AtomicInteger refusedWrites = new AtomicInteger();

    /**
     * Breaks or repairs the three parts of the storage together.
     */
    public void setBroken(boolean broken) {
        logBroken = broken;
        snapshotsBroken = broken;
        metadataBroken = broken;
    }

    public void setLogBroken(boolean broken) {
        logBroken = broken;
    }

    public void setSnapshotsBroken(boolean broken) {
        snapshotsBroken = broken;
    }

    public void setMetadataBroken(boolean broken) {
        metadataBroken = broken;
    }

    /**
     * @return how many writes were refused so far
     */
    public int refusedWrites() {
        return refusedWrites.get();
    }

    private void refuseIf(boolean broken, String operation) {
        if (broken) {
            refusedWrites.incrementAndGet();
            throw new UncheckedIOException(new IOException("Injected storage failure: " + operation));
        }
    }

    public RaftStorage wrap(RaftStorage storage) {
        return new RaftStorage(wrap(storage.log()), wrap(storage.snapshots()), wrap(storage.metadata()));
    }

    private RaftLog wrap(RaftLog log) {
        return new RaftLog() {
            @Override
            public void append(LogEntry entry) {
                refuseIf(logBroken, "append to the log");
                log.append(entry);
            }

            @Override
            public void append(List<LogEntry> entries) {
                refuseIf(logBroken, "append to the log");
                log.append(entries);
            }

            @Override
            public LogEntry getEntry(long index) {
                return log.getEntry(index);
            }

            @Override
            public List<LogEntry> getEntriesFrom(long index) {
                return log.getEntriesFrom(index);
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
                refuseIf(logBroken, "truncate the log");
                log.truncateFrom(index);
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
                refuseIf(logBroken, "compact the log");
                log.compact(upToIndex, upToTerm);
            }

            @Override
            public void reset(long baseIndex, long baseTerm) {
                refuseIf(logBroken, "reset the log");
                log.reset(baseIndex, baseTerm);
            }

            @Override
            public void sync() {
                refuseIf(logBroken, "flush the log");
                log.sync();
            }

            @Override
            public void recover() {
                refuseIf(logBroken, "rewrite the log");
                log.recover();
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

    private SnapshotStore wrap(SnapshotStore store) {
        return new SnapshotStore() {
            @Override
            public Snapshot load() {
                return store.load();
            }

            @Override
            public void save(Snapshot snapshot) {
                refuseIf(snapshotsBroken, "save a snapshot");
                store.save(snapshot);
            }

            @Override
            public void verifyWritable() {
                refuseIf(snapshotsBroken, "write to the snapshot store");
                store.verifyWritable();
            }
        };
    }

    private PersistentMetadata wrap(PersistentMetadata metadata) {
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
                refuseIf(metadataBroken, "save term and vote");
                metadata.save(term, votedFor);
            }
        };
    }
}
