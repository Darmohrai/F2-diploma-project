package org.kurin.raft.snapshot;

import org.kurin.network.serializer.KryoSerializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/**
 * Stores the snapshot in {@code snapshot.bin} as {@code [length:int][crc32:int][kryo bytes]}.
 * A new snapshot is written to a temporary file and moved over the old one, so a crash leaves either
 * the old snapshot or the new one, never a mixture.
 */
public class FileSnapshotStore implements SnapshotStore {
    private static final String FILE_NAME = "snapshot.bin";

    private final Path file;
    private final Path tempFile;
    private final KryoSerializer serializer;
    private Snapshot cached;

    public FileSnapshotStore(Path directory, KryoSerializer serializer) {
        this.file = directory.resolve(FILE_NAME);
        this.tempFile = directory.resolve(FILE_NAME + ".tmp");
        this.serializer = serializer;
        try {
            Files.createDirectories(directory);
            Files.deleteIfExists(tempFile);
            this.cached = Files.exists(file) ? read() : null;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open the snapshot store in " + directory, e);
        }
    }

    private Snapshot read() throws IOException {
        byte[] content = Files.readAllBytes(file);
        if (content.length < 8) {
            throw corrupt("file is shorter than its header");
        }
        ByteBuffer buffer = ByteBuffer.wrap(content);
        int length = buffer.getInt();
        int expectedCrc = buffer.getInt();
        if (length != content.length - 8) {
            throw corrupt("declared length " + length + " does not match the file size");
        }
        CRC32 crc = new CRC32();
        crc.update(content, 8, length);
        if ((int) crc.getValue() != expectedCrc) {
            throw corrupt("checksum mismatch");
        }
        return (Snapshot) serializer.fromBytes(content, 8, length);
    }

    private IllegalStateException corrupt(String reason) {
        return new IllegalStateException("Snapshot " + file + " is corrupt (" + reason + "). The log entries it replaced are gone, "
                + "so this node cannot recover on its own: delete its data directory and let it resynchronise from the cluster.");
    }

    @Override
    public Snapshot load() {
        return cached;
    }

    @Override
    public void save(Snapshot snapshot) {
        byte[] payload = serializer.toBytes(snapshot);
        CRC32 crc = new CRC32();
        crc.update(payload);
        try {
            try (FileChannel channel = FileChannel.open(tempFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                ByteBuffer buffer = ByteBuffer.allocate(8 + payload.length);
                buffer.putInt(payload.length).putInt((int) crc.getValue()).put(payload).flip();
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            try {
                Files.move(tempFile, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tempFile, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot save snapshot " + snapshot.lastIncludedIndex() + " to " + file, e);
        }
        this.cached = snapshot;
    }

    @Override
    public void verifyWritable() {
        Path probe = tempFile.resolveSibling(tempFile.getFileName() + ".probe");
        try {
            try (FileChannel channel = FileChannel.open(probe, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                channel.write(ByteBuffer.wrap(new byte[]{1}));
                channel.force(true);
            }
            Files.delete(probe);
        } catch (IOException e) {
            throw new UncheckedIOException("The snapshot store in " + file.getParent() + " does not accept writes", e);
        }
    }
}
