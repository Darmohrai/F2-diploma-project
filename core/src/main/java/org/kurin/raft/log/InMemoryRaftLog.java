package org.kurin.raft.log;

import java.util.ArrayList;
import java.util.List;

public class InMemoryRaftLog implements RaftLog {
    private final List<LogEntry> entries = new ArrayList<>();
    private long baseIndex = 0;
    private long baseTerm = 0;

    @Override
    public void append(LogEntry entry) {
        if (entry.index() != getLastLogIndex() + 1) {
            throw new IllegalArgumentException(
                    "Log integrity violation. Index expected " + (getLastLogIndex() + 1) + ", get " + entry.index()
            );
        }
        entries.add(entry);
    }

    @Override
    public void append(List<LogEntry> newEntries) {
        for (LogEntry entry : newEntries) {
            append(entry);
        }
    }

    @Override
    public LogEntry getEntry(long index) {
        if (index <= baseIndex || index > getLastLogIndex()) {
            return null;
        }
        return entries.get((int) (index - baseIndex - 1));
    }

    @Override
    public List<LogEntry> getEntriesFrom(long index) {
        if (index <= baseIndex) {
            index = baseIndex + 1;
        }
        if (index > getLastLogIndex()) {
            return new ArrayList<>();
        }
        int listIndex = (int) (index - baseIndex - 1);
        return new ArrayList<>(entries.subList(listIndex, entries.size()));
    }

    @Override
    public long getLastLogIndex() {
        return baseIndex + entries.size();
    }

    @Override
    public long getLastLogTerm() {
        if (entries.isEmpty()) {
            return baseTerm;
        }
        return entries.getLast().term();
    }

    @Override
    public void truncateFrom(long index) {
        if (index <= baseIndex || index > getLastLogIndex()) {
            return;
        }
        int listIndex = (int) (index - baseIndex - 1);
        entries.subList(listIndex, entries.size()).clear();
    }

    @Override
    public long getBaseIndex() {
        return baseIndex;
    }

    @Override
    public long getBaseTerm() {
        return baseTerm;
    }

    @Override
    public void compact(long upToIndex, long upToTerm) {
        if (upToIndex <= baseIndex) return;

        int removeCount = (int) (upToIndex - baseIndex);
        if (removeCount >= entries.size()) {
            entries.clear();
        } else {
            entries.subList(0, removeCount).clear();
        }

        this.baseIndex = upToIndex;
        this.baseTerm = upToTerm;
    }
}
