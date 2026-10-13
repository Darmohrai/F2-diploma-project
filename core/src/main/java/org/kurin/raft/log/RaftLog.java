package org.kurin.raft.log;

import java.util.List;

/**
 * The Raft log of one node. Entries up to {@link #getBaseIndex()} are covered by a snapshot and no longer stored.
 * Used from the Raft thread only.
 */
public interface RaftLog extends AutoCloseable {

    void append(LogEntry entry);

    void append(List<LogEntry> entries);

    /**
     * @return the entry, or null if the index is compacted away or beyond the end of the log
     */
    LogEntry getEntry(long index);

    List<LogEntry> getEntriesFrom(long index);

    long getLastLogIndex();

    long getLastLogTerm();

    /**
     * Removes the entry at {@code index} and everything after it.
     */
    void truncateFrom(long index);

    long getBaseIndex();

    long getBaseTerm();

    /**
     * Drops entries up to and including {@code upToIndex}, which a snapshot now covers. Later entries are kept.
     */
    void compact(long upToIndex, long upToTerm);

    /**
     * Discards the whole log and starts over after a snapshot whose last entry it does not contain.
     */
    void reset(long baseIndex, long baseTerm);

    /**
     * Makes every appended entry durable. Appends are buffered until this is called, so that many entries
     * cost one disk flush.
     */
    void sync();

    /**
     * Brings the stored log back in line with the entries this object holds, after an operation failed with an
     * I/O error. Until it returns normally, nothing appended since the last successful {@link #sync()} may be
     * assumed durable. The default suits logs that keep nothing outside of memory.
     *
     * @throws java.io.UncheckedIOException if the storage still does not work
     */
    default void recover() {
        sync();
    }

    /**
     * @return the highest index known to survive a crash; equals {@link #getLastLogIndex()} right after {@link #sync()}
     */
    long getDurableIndex();

    @Override
    void close();
}
