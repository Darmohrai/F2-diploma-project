package org.kurin.raft.state;

import org.kurin.network.model.NodeAddress;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class FilePersistentMetadata implements PersistentMetadata {
    private final Path filePath;
    private final Path tempPath;
    private long currentTerm = 0;
    private NodeAddress votedFor = null;

    public FilePersistentMetadata(Path dataDir) {
        this.filePath = dataDir.resolve("metadata.bin");
        this.tempPath = dataDir.resolve("metadata.bin.tmp");
        try {
            Files.createDirectories(dataDir);
            if (Files.exists(filePath)) {
                loadFromDisk();
            } else {
                save(0, null);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot initialize Raft metadata in " + dataDir, e);
        }
    }

    private void loadFromDisk() throws IOException {
        try (DataInputStream in = new DataInputStream(Files.newInputStream(filePath))) {
            this.currentTerm = in.readLong();
            if (in.readBoolean()) {
                String host = in.readUTF();
                int port = in.readInt();
                this.votedFor = new NodeAddress(host, port);
            }
        }
    }

    @Override
    public long getCurrentTerm() {
        return currentTerm;
    }

    @Override
    public NodeAddress getVotedFor() {
        return votedFor;
    }

    @Override
    public void save(long term, NodeAddress votedFor) {
        try {
            try (FileOutputStream fos = new FileOutputStream(tempPath.toFile());
                 DataOutputStream out = new DataOutputStream(fos)) {
                out.writeLong(term);
                out.writeBoolean(votedFor != null);
                if (votedFor != null) {
                    out.writeUTF(votedFor.host());
                    out.writeInt(votedFor.port());
                }
                out.flush();
                fos.getFD().sync();
            }
            try {
                Files.move(tempPath, filePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tempPath, filePath, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot persist Raft term and vote to " + filePath
                    + ". The node cannot safely take part in elections.", e);
        }
        this.currentTerm = term;
        this.votedFor = votedFor;
    }
}
