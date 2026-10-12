package org.kurin.raft.snapshot;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.kurin.network.model.NodeAddress;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.raft.state.CommandOutcome;
import org.kurin.raft.state.FilePersistentMetadata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DurableStateTest {
    private final KryoSerializer serializer = KryoSerializer.standalone();

    @TempDir
    Path directory;

    private static Snapshot snapshot(long index, String state) {
        LinkedHashMap<String, CommandOutcome> dedup = new LinkedHashMap<>();
        dedup.put("request-a", CommandOutcome.success(41));
        dedup.put("request-b", CommandOutcome.failure(IllegalStateException.class.getName(), "boom"));
        return new Snapshot(index, 3, state.getBytes(), dedup);
    }

    @Test
    void snapshotSurvivesReopenAndIsReplacedAtomically() {
        FileSnapshotStore store = new FileSnapshotStore(directory, serializer);
        assertThat(store.load()).isNull();

        store.save(snapshot(100, "first"));
        store.save(snapshot(200, "second"));

        Snapshot loaded = new FileSnapshotStore(directory, serializer).load();
        assertThat(loaded.lastIncludedIndex()).isEqualTo(200);
        assertThat(loaded.lastIncludedTerm()).isEqualTo(3);
        assertThat(loaded.data()).isEqualTo("second".getBytes());
        assertThat(loaded.dedupTable()).containsExactly(
                java.util.Map.entry("request-a", CommandOutcome.success(41)),
                java.util.Map.entry("request-b", CommandOutcome.failure(IllegalStateException.class.getName(), "boom")));
        assertThat(directory.resolve("snapshot.bin.tmp")).doesNotExist();
    }

    @Test
    void corruptSnapshotIsReportedInsteadOfLoaded() throws IOException {
        new FileSnapshotStore(directory, serializer).save(snapshot(100, "state"));
        Path file = directory.resolve("snapshot.bin");
        byte[] bytes = Files.readAllBytes(file);
        bytes[bytes.length - 1] ^= 0x01;
        Files.write(file, bytes);

        assertThatThrownBy(() -> new FileSnapshotStore(directory, serializer))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("corrupt")
                .hasMessageContaining("delete its data directory");
    }

    @Test
    void termAndVoteAreOverwrittenOnEverySave() {
        FilePersistentMetadata metadata = new FilePersistentMetadata(directory);
        metadata.save(1, new NodeAddress("10.0.0.1", 9000));
        // The second save must replace the file of the first one (File.renameTo did not on Windows)
        metadata.save(2, new NodeAddress("10.0.0.2", 9000));
        metadata.save(3, null);
        metadata.save(4, new NodeAddress("10.0.0.3", 9001));

        FilePersistentMetadata reopened = new FilePersistentMetadata(directory);
        assertThat(reopened.getCurrentTerm()).isEqualTo(4);
        assertThat(reopened.getVotedFor()).isEqualTo(new NodeAddress("10.0.0.3", 9001));
    }
}
