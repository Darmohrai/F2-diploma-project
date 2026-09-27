package org.kurin.raft.log;

import org.kurin.network.serializer.KryoSerializer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class FileRaftLog implements RaftLog {
    private final List<LogEntry> entries = new ArrayList<>();
    private final Path logPath;
    private final KryoSerializer serializer;
    private FileOutputStream fileOut;

    private long baseIndex = 0;
    private long baseTerm = 0;

    public FileRaftLog(String nodeId, KryoSerializer serializer) {
        this.logPath = Path.of("raft-data-" + nodeId, "raft.log");
        this.serializer = serializer;

        try {
            Files.createDirectories(logPath.getParent());
            if (Files.exists(logPath)) {
                loadLogFromDisk();
            }
            this.fileOut = new FileOutputStream(logPath.toFile(), true);
        } catch (IOException e) {
            throw new RuntimeException("Failed to init FileRaftLog", e);
        }
    }

    private void loadLogFromDisk() {
        try (FileInputStream fis = new FileInputStream(logPath.toFile())) {
            while (fis.available() > 0) {
                LogEntry entry = (LogEntry) serializer.deserialize(fis);
                entries.add(entry);
            }
            if (!entries.isEmpty()) {
                baseIndex = entries.getFirst().index() - 1;
            }
        } catch (Exception e) {
        }
    }

    @Override
    public void append(LogEntry entry) {
        if (entry.index() != getLastLogIndex() + 1) {
            throw new IllegalArgumentException(
                    "Log integrity violation. Expected " + (getLastLogIndex() + 1) + " but got " + entry.index()
            );
        }
        entries.add(entry);
        try {
            serializer.serialize(entry, fileOut);
            fileOut.getFD().sync();
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush log entry", e);
        }
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
        if (index <= baseIndex || index > getLastLogIndex()) return;

        int listIndex = (int) (index - baseIndex - 1);
        entries.subList(listIndex, entries.size()).clear();
        rewriteLogFile();
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

        rewriteLogFile();
    }

    private void rewriteLogFile() {
        try {
            fileOut.close();
            Files.deleteIfExists(logPath);
            fileOut = new FileOutputStream(logPath.toFile(), true);
            for (LogEntry entry : entries) {
                serializer.serialize(entry, fileOut);
            }
            fileOut.getFD().sync();
        } catch (IOException e) {
            throw new RuntimeException("Failed to rewrite log file", e);
        }
    }
}