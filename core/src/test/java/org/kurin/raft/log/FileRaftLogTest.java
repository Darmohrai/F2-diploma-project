package org.kurin.raft.log;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.kurin.network.serializer.KryoSerializer;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class FileRaftLogTest {
    private final KryoSerializer serializer = KryoSerializer.standalone();

    @TempDir
    Path directory;

    private FileRaftLog open() {
        // Tiny segments force several files
        return new FileRaftLog(directory, serializer, 256);
    }

    private static LogEntry entry(long index, long term) {
        return new LogEntry(index, term, LogEntryType.USER, "request-" + index, 1_000 + index, "command-" + index);
    }

    private static void append(RaftLog log, long from, long to, long term) {
        for (long index = from; index <= to; index++) {
            log.append(entry(index, term));
        }
        log.sync();
    }

    private List<Path> segmentFiles() throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(file -> file.getFileName().toString().startsWith("segment-")).sorted().toList();
        }
    }

    @Test
    void recoverRewritesDamagedFilesFromMemory() throws IOException {
        try (FileRaftLog log = open()) {
            append(log, 1, 30, 2);
            log.compact(10, 2);
            log.append(entry(31, 2)); // appended, never flushed

            // What a failed disk leaves behind: a torn record at the end and a segment that is gone
            List<Path> segments = segmentFiles();
            assertThat(segments).hasSizeGreaterThan(2);
            Files.write(segments.getLast(), new byte[]{0, 0, 0, 9, 1, 2, 3}, StandardOpenOption.APPEND);
            Files.delete(segments.get(1));

            log.recover();

            assertThat(log.getDurableIndex()).isEqualTo(31);
            assertThat(segmentFiles()).hasSize(1);
            log.append(entry(32, 2));
            log.sync();
        }
        try (FileRaftLog reopened = open()) {
            assertThat(reopened.getBaseIndex()).isEqualTo(10);
            assertThat(reopened.getBaseTerm()).isEqualTo(2);
            assertThat(reopened.getLastLogIndex()).isEqualTo(32);
            assertThat(reopened.getEntriesFrom(11)).extracting(LogEntry::index)
                    .containsExactlyElementsOf(java.util.stream.LongStream.rangeClosed(11, 32).boxed().toList());
            assertThat(reopened.getEntry(31).command()).isEqualTo("command-31");
        }
    }

    @Test
    void entriesSurviveReopen() throws IOException {
        try (FileRaftLog log = open()) {
            append(log, 1, 50, 3);
            assertThat(log.getDurableIndex()).isEqualTo(50);
        }
        assertThat(segmentFiles()).hasSizeGreaterThan(1);

        try (FileRaftLog reopened = open()) {
            assertThat(reopened.getLastLogIndex()).isEqualTo(50);
            assertThat(reopened.getLastLogTerm()).isEqualTo(3);
            assertThat(reopened.getEntry(17)).isEqualTo(entry(17, 3));
            assertThat(reopened.getEntriesFrom(48)).containsExactly(entry(48, 3), entry(49, 3), entry(50, 3));
        }
    }

    @Test
    void appendedEntriesAreNotDurableUntilSync() {
        try (FileRaftLog log = open()) {
            log.append(entry(1, 1));
            log.append(entry(2, 1));
            assertThat(log.getLastLogIndex()).isEqualTo(2);
            assertThat(log.getDurableIndex()).isZero();

            log.sync();
            assertThat(log.getDurableIndex()).isEqualTo(2);
        }
    }

    // Spec test 8, second half
    @Test
    void tornTailIsCutOffOnRecovery() throws IOException {
        try (FileRaftLog log = open()) {
            append(log, 1, 30, 1);
        }
        Path lastSegment = segmentFiles().getLast();
        try (FileChannel channel = FileChannel.open(lastSegment, StandardOpenOption.WRITE)) {
            channel.truncate(channel.size() - 5); // the last record loses its end
        }

        try (FileRaftLog recovered = open()) {
            assertThat(recovered.getLastLogIndex()).isEqualTo(29);
            assertThat(recovered.getEntry(29)).isEqualTo(entry(29, 1));

            // The log accepts new entries right after the surviving ones
            append(recovered, 30, 31, 2);
        }
        try (FileRaftLog reopened = open()) {
            assertThat(reopened.getLastLogIndex()).isEqualTo(31);
            assertThat(reopened.getEntry(30)).isEqualTo(entry(30, 2));
        }
    }

    @Test
    void corruptRecordDropsItAndEverythingAfter() throws IOException {
        try (FileRaftLog log = open()) {
            append(log, 1, 30, 1);
        }
        List<Path> segments = segmentFiles();
        Path damaged = segments.get(1);
        byte[] bytes = Files.readAllBytes(damaged);
        bytes[bytes.length / 2] ^= 0x55;
        Files.write(damaged, bytes);
        long firstIndexOfDamaged = Long.parseLong(damaged.getFileName().toString().replaceAll("\\D", ""));

        try (FileRaftLog recovered = open()) {
            assertThat(recovered.getLastLogIndex()).isGreaterThanOrEqualTo(firstIndexOfDamaged - 1).isLessThan(30);
            long last = recovered.getLastLogIndex();
            assertThat(recovered.getEntry(last)).isEqualTo(entry(last, 1));
        }
        // Segments after the damaged one are gone
        assertThat(segmentFiles()).hasSizeLessThanOrEqualTo(2);
    }

    @Test
    void truncateFromRemovesTailAcrossSegments() throws IOException {
        try (FileRaftLog log = open()) {
            append(log, 1, 40, 1);
            log.truncateFrom(12);
            assertThat(log.getLastLogIndex()).isEqualTo(11);
            append(log, 12, 15, 2);
        }
        try (FileRaftLog reopened = open()) {
            assertThat(reopened.getLastLogIndex()).isEqualTo(15);
            assertThat(reopened.getEntry(11)).isEqualTo(entry(11, 1));
            assertThat(reopened.getEntry(12)).isEqualTo(entry(12, 2));
        }
    }

    @Test
    void compactionKeepsTheTailAndSurvivesReopen() throws IOException {
        try (FileRaftLog log = open()) {
            append(log, 1, 40, 4);
            int segmentsBefore = segmentFiles().size();

            log.compact(25, 4);

            assertThat(log.getBaseIndex()).isEqualTo(25);
            assertThat(log.getBaseTerm()).isEqualTo(4);
            assertThat(log.getEntry(25)).isNull();
            assertThat(log.getEntry(26)).isEqualTo(entry(26, 4));
            assertThat(log.getLastLogIndex()).isEqualTo(40);
            assertThat(segmentFiles()).hasSizeLessThan(segmentsBefore);
        }
        try (FileRaftLog reopened = open()) {
            assertThat(reopened.getBaseIndex()).isEqualTo(25);
            assertThat(reopened.getBaseTerm()).isEqualTo(4);
            assertThat(reopened.getLastLogIndex()).isEqualTo(40);
            assertThat(reopened.getEntriesFrom(1)).hasSize(15).first().isEqualTo(entry(26, 4));
        }
    }

    @Test
    void compactingEverythingLeavesAnEmptyLogAtTheBase() {
        try (FileRaftLog log = open()) {
            append(log, 1, 10, 2);
            log.compact(10, 2);

            assertThat(log.getLastLogIndex()).isEqualTo(10);
            assertThat(log.getLastLogTerm()).isEqualTo(2);
            append(log, 11, 12, 3);
        }
        try (FileRaftLog reopened = open()) {
            assertThat(reopened.getBaseIndex()).isEqualTo(10);
            assertThat(reopened.getLastLogIndex()).isEqualTo(12);
            assertThat(reopened.getEntry(11)).isEqualTo(entry(11, 3));
        }
    }

    @Test
    void resetStartsOverAfterASnapshot() {
        try (FileRaftLog log = open()) {
            append(log, 1, 20, 1);
            log.reset(500, 7);

            assertThat(log.getBaseIndex()).isEqualTo(500);
            assertThat(log.getLastLogIndex()).isEqualTo(500);
            assertThat(log.getLastLogTerm()).isEqualTo(7);
            append(log, 501, 503, 7);
        }
        try (FileRaftLog reopened = open()) {
            assertThat(reopened.getBaseIndex()).isEqualTo(500);
            assertThat(reopened.getLastLogIndex()).isEqualTo(503);
            assertThat(reopened.getEntry(20)).isNull();
            assertThat(reopened.getEntry(502)).isEqualTo(entry(502, 7));
        }
    }
}
