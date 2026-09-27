package org.kurin.raft.log;

import java.util.List;

public interface RaftLog {

    void append(LogEntry entry);

    void append(List<LogEntry> entries);

    LogEntry getEntry(long index);

    List<LogEntry> getEntriesFrom(long index);

    long getLastLogIndex();

    long getLastLogTerm();

    void truncateFrom(long index);

    long getBaseIndex();

    long getBaseTerm();

    void compact(long upToIndex, long upToTerm);
}
