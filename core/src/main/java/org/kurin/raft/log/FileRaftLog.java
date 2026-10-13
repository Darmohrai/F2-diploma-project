package org.kurin.raft.log;

import org.kurin.network.serializer.KryoSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.zip.CRC32;

/**
 * Raft log stored in segment files, with all live entries also kept in memory.
 * <p>
 * Layout of the directory:
 * <ul>
 *   <li>{@code segment-<first index>.log}: records {@code [length:int][crc32:int][kryo bytes]}, in index order;</li>
 *   <li>{@code log.meta}: {@code baseIndex} and {@code baseTerm}, the position of the snapshot the log starts after.</li>
 * </ul>
 * Compaction only rewrites {@code log.meta} and deletes whole segments; entries at or below the base that remain
 * in a segment are skipped when the log is read back. A torn or corrupt tail found at startup is cut off.
 */
public class FileRaftLog implements RaftLog {
    private static final Logger log = LoggerFactory.getLogger(FileRaftLog.class);

    private static final String SEGMENT_PREFIX = "segment-";
    private static final String SEGMENT_SUFFIX = ".log";
    private static final String META_FILE = "log.meta";
    private static final int RECORD_HEADER_BYTES = 8;
    public static final long DEFAULT_SEGMENT_BYTES = 8L * 1024 * 1024;

    private final Path directory;
    private final KryoSerializer serializer;
    private final long segmentMaxBytes;

    private final List<LogEntry> entries = new ArrayList<>();
    // First index of every segment file on disk
    private final TreeMap<Long, Path> segments = new TreeMap<>();
    private long baseIndex;
    private long baseTerm;
    private long durableIndex;

    private FileChannel activeChannel;
    private long activeSegmentBytes;
    private boolean unsynced;

    public FileRaftLog(Path directory, KryoSerializer serializer) {
        this(directory, serializer, DEFAULT_SEGMENT_BYTES);
    }

    public FileRaftLog(Path directory, KryoSerializer serializer, long segmentMaxBytes) {
        this.directory = directory;
        this.serializer = serializer;
        this.segmentMaxBytes = segmentMaxBytes;
        try {
            Files.createDirectories(directory);
            readMeta();
            readSegments();
            durableIndex = getLastLogIndex();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open the Raft log in " + directory, e);
        }
    }

    // ---------------------------------------------------------------- recovery

    private void readMeta() throws IOException {
        Path meta = directory.resolve(META_FILE);
        if (!Files.exists(meta)) {
            return;
        }
        try (DataInputStream in = new DataInputStream(Files.newInputStream(meta))) {
            baseIndex = in.readLong();
            baseTerm = in.readLong();
        }
    }

    private void readSegments() throws IOException {
        TreeMap<Long, Path> found = new TreeMap<>();
        try (Stream<Path> files = Files.list(directory)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                String name = file.getFileName().toString();
                if (name.startsWith(SEGMENT_PREFIX) && name.endsWith(SEGMENT_SUFFIX)) {
                    found.put(Long.parseLong(name.substring(SEGMENT_PREFIX.length(), name.length() - SEGMENT_SUFFIX.length())), file);
                } else if (name.endsWith(".tmp")) {
                    Files.delete(file); // leftover of an interrupted atomic write
                }
            }
        }

        boolean damaged = false;
        for (var segment : found.entrySet()) {
            Path file = segment.getValue();
            if (damaged) {
                log.warn("Dropping Raft log segment {} that follows a damaged one", file.getFileName());
                Files.delete(file);
                continue;
            }
            long validBytes = readSegment(file);
            if (validBytes < Files.size(file)) {
                log.warn("Raft log segment {} has a damaged tail. Cutting it at byte {} of {}; {} entries are kept in total.",
                        file.getFileName(), validBytes, Files.size(file), entries.size());
                try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
                    channel.truncate(validBytes);
                    channel.force(true);
                }
                damaged = true;
            }
            segments.put(segment.getKey(), file);
        }

        // Segments fully covered by the snapshot are no longer needed
        while (segments.size() > 1 && segments.higherKey(segments.firstKey()) <= baseIndex + 1) {
            Files.delete(segments.pollFirstEntry().getValue());
        }

        if (!segments.isEmpty()) {
            Path last = segments.lastEntry().getValue();
            activeChannel = FileChannel.open(last, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            activeSegmentBytes = Files.size(last);
        }
    }

    /**
     * Reads entries of one segment into memory in a single buffered pass.
     *
     * @return the number of leading bytes that hold intact, contiguous records
     */
    private long readSegment(Path file) throws IOException {
        long validBytes = 0;
        long fileBytes = Files.size(file);
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(file), 1 << 16))) {
            while (true) {
                int length;
                try {
                    length = in.readInt();
                } catch (EOFException endOfSegment) {
                    return validBytes;
                }
                int expectedCrc = in.readInt();
                if (length <= 0 || length > fileBytes - validBytes - RECORD_HEADER_BYTES) {
                    return validBytes;
                }
                byte[] payload = new byte[length];
                in.readFully(payload);
                if (crcOf(payload) != expectedCrc) {
                    return validBytes;
                }
                LogEntry entry = (LogEntry) serializer.fromBytes(payload);
                if (entry.index() > baseIndex) {
                    if (entry.index() != getLastLogIndex() + 1) {
                        // A gap means a truncation was interrupted: everything from here on is stale
                        return validBytes;
                    }
                    entries.add(entry);
                }
                validBytes += RECORD_HEADER_BYTES + length;
            }
        } catch (EOFException tornRecord) {
            return validBytes;
        } catch (RuntimeException undecodable) {
            log.warn("Raft log segment {} holds an undecodable record: {}", file.getFileName(), undecodable.getMessage());
            return validBytes;
        }
    }

    // ---------------------------------------------------------------- appending

    @Override
    public void append(LogEntry entry) {
        if (entry.index() != getLastLogIndex() + 1) {
            throw new IllegalArgumentException(
                    "Log integrity violation: expected index " + (getLastLogIndex() + 1) + ", got " + entry.index());
        }
        byte[] payload = serializer.toBytes(entry);
        try {
            if (activeChannel == null || activeSegmentBytes >= segmentMaxBytes) {
                rollSegment(entry.index());
            }
            ByteBuffer record = ByteBuffer.allocate(RECORD_HEADER_BYTES + payload.length);
            record.putInt(payload.length).putInt(crcOf(payload)).put(payload).flip();
            while (record.hasRemaining()) {
                activeChannel.write(record);
            }
            activeSegmentBytes += RECORD_HEADER_BYTES + payload.length;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot append entry " + entry.index() + " to the Raft log in " + directory, e);
        }
        entries.add(entry);
        unsynced = true;
    }

    @Override
    public void append(List<LogEntry> newEntries) {
        for (LogEntry entry : newEntries) {
            append(entry);
        }
    }

    private void rollSegment(long firstIndex) throws IOException {
        if (activeChannel != null) {
            activeChannel.force(true);
            activeChannel.close();
        }
        Path file = segmentPath(firstIndex);
        activeChannel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        activeSegmentBytes = 0;
        segments.put(firstIndex, file);
    }

    @Override
    public void sync() {
        if (!unsynced) {
            return;
        }
        try {
            activeChannel.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot flush the Raft log in " + directory, e);
        }
        unsynced = false;
        durableIndex = getLastLogIndex();
    }

    /**
     * After a failed write the files can hold a torn record, lack a segment or carry a base that was never
     * applied, and data that a failed flush dropped is not written by flushing again. So the files are not
     * repaired: the entries held in memory are written out afresh as one segment, flushed, and only then put
     * in place of everything else.
     */
    @Override
    public void recover() {
        try {
            if (activeChannel != null) {
                try {
                    activeChannel.close();
                } catch (IOException e) {
                    log.debug("Closing the damaged segment of the Raft log in {} failed: {}", directory, e.toString());
                }
                activeChannel = null;
            }
            writeMeta(baseIndex, baseTerm);
            Path rewritten = null;
            if (!entries.isEmpty()) {
                rewritten = segmentPath(baseIndex + 1);
                writeSegmentAtomically(rewritten, entries);
            }
            try (Stream<Path> files = Files.list(directory)) {
                for (Path file : (Iterable<Path>) files::iterator) {
                    String name = file.getFileName().toString();
                    boolean segment = name.startsWith(SEGMENT_PREFIX) && name.endsWith(SEGMENT_SUFFIX);
                    if ((segment && !file.equals(rewritten)) || name.endsWith(".tmp")) {
                        Files.delete(file);
                    }
                }
            }
            segments.clear();
            if (rewritten != null) {
                segments.put(baseIndex + 1, rewritten);
            }
            reopenActive();
        } catch (IOException e) {
            throw new UncheckedIOException("The Raft log in " + directory + " still cannot be written", e);
        }
        unsynced = false;
        durableIndex = getLastLogIndex();
        log.info("Raft log in {} was rewritten from memory: {} entries after index {}", directory, entries.size(), baseIndex);
    }

    @Override
    public long getDurableIndex() {
        return durableIndex;
    }

    // ---------------------------------------------------------------- reading

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
        return new ArrayList<>(entries.subList((int) (index - baseIndex - 1), entries.size()));
    }

    @Override
    public long getLastLogIndex() {
        return baseIndex + entries.size();
    }

    @Override
    public long getLastLogTerm() {
        return entries.isEmpty() ? baseTerm : entries.getLast().term();
    }

    @Override
    public long getBaseIndex() {
        return baseIndex;
    }

    @Override
    public long getBaseTerm() {
        return baseTerm;
    }

    // ---------------------------------------------------------------- truncation and compaction

    @Override
    public void truncateFrom(long index) {
        if (index <= baseIndex || index > getLastLogIndex()) {
            return;
        }
        try {
            closeActive();
            // 1. The segment holding the cut point is replaced atomically by its prefix
            Long boundary = segments.floorKey(index);
            if (boundary != null) {
                long firstKept = Math.max(boundary, baseIndex + 1);
                List<LogEntry> prefix = new ArrayList<>(entries.subList((int) (firstKept - baseIndex - 1), (int) (index - baseIndex - 1)));
                if (prefix.isEmpty()) {
                    Files.delete(segments.remove(boundary));
                } else {
                    writeSegmentAtomically(segments.get(boundary), prefix);
                }
            }
            // 2. Later segments go away; if a crash leaves some behind, recovery sees the gap and drops them
            for (Long later : new ArrayList<>(segments.tailMap(index, true).keySet())) {
                Files.delete(segments.remove(later));
            }
            entries.subList((int) (index - baseIndex - 1), entries.size()).clear();
            reopenActive();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot truncate the Raft log in " + directory + " from index " + index, e);
        }
        durableIndex = Math.min(durableIndex, getLastLogIndex());
        unsynced = false;
    }

    @Override
    public void compact(long upToIndex, long upToTerm) {
        if (upToIndex <= baseIndex) {
            return;
        }
        try {
            sync();
            int removeCount = (int) Math.min(upToIndex - baseIndex, entries.size());
            // The new base is durable first: from now on the dropped entries are ignored even if their files remain
            writeMeta(upToIndex, upToTerm);
            entries.subList(0, removeCount).clear();
            baseIndex = upToIndex;
            baseTerm = upToTerm;

            while (segments.size() > 1 && segments.higherKey(segments.firstKey()) <= baseIndex + 1) {
                Files.delete(segments.pollFirstEntry().getValue());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot compact the Raft log in " + directory, e);
        }
        durableIndex = Math.max(durableIndex, baseIndex);
    }

    @Override
    public void reset(long newBaseIndex, long newBaseTerm) {
        try {
            closeActive();
            writeMeta(newBaseIndex, newBaseTerm);
            for (Path file : segments.values()) {
                Files.delete(file);
            }
            segments.clear();
            entries.clear();
            baseIndex = newBaseIndex;
            baseTerm = newBaseTerm;
            activeSegmentBytes = 0;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot reset the Raft log in " + directory, e);
        }
        durableIndex = baseIndex;
        unsynced = false;
    }

    private void writeMeta(long index, long term) throws IOException {
        Path temp = directory.resolve(META_FILE + ".tmp");
        try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putLong(index).putLong(term).flip();
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
        moveAtomically(temp, directory.resolve(META_FILE));
    }

    private void writeSegmentAtomically(Path target, List<LogEntry> content) throws IOException {
        Path temp = target.resolveSibling(target.getFileName() + ".tmp");
        try (RandomAccessFile file = new RandomAccessFile(temp.toFile(), "rw")) {
            file.setLength(0);
            DataOutputStream out = new DataOutputStream(new java.io.BufferedOutputStream(new java.io.FileOutputStream(file.getFD()), 1 << 16));
            for (LogEntry entry : content) {
                byte[] payload = serializer.toBytes(entry);
                out.writeInt(payload.length);
                out.writeInt(crcOf(payload));
                out.write(payload);
            }
            out.flush();
            file.getFD().sync();
        }
        moveAtomically(temp, target);
    }

    private static void moveAtomically(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void closeActive() throws IOException {
        if (activeChannel != null) {
            activeChannel.force(true);
            activeChannel.close();
            activeChannel = null;
        }
    }

    private void reopenActive() throws IOException {
        if (segments.isEmpty()) {
            activeSegmentBytes = 0;
            return;
        }
        Path last = segments.lastEntry().getValue();
        activeChannel = FileChannel.open(last, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        activeSegmentBytes = Files.size(last);
    }

    private Path segmentPath(long firstIndex) {
        return directory.resolve(String.format("%s%020d%s", SEGMENT_PREFIX, firstIndex, SEGMENT_SUFFIX));
    }

    private static int crcOf(byte[] payload) {
        CRC32 crc = new CRC32();
        crc.update(payload);
        return (int) crc.getValue();
    }

    @Override
    public void close() {
        try {
            closeActive();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot close the Raft log in " + directory, e);
        }
    }
}
