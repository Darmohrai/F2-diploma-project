package org.kurin.raft.state;

import org.kurin.network.model.NodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

public class FilePersistentMetadata implements PersistentMetadata {
    private static final Logger log = LoggerFactory.getLogger(FilePersistentMetadata.class);

    private final Path filePath;
    private long currentTerm = 0;
    private NodeAddress votedFor = null;

    public FilePersistentMetadata(String nodeId) {
        this.filePath = Path.of("raft-data-" + nodeId, "metadata.bin");
        try {
            Files.createDirectories(filePath.getParent());
            if (Files.exists(filePath)) {
                loadFromDisk();
            } else {
                flush();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize persistent metadata", e);
        }
    }

    private void loadFromDisk() throws IOException {
        try (DataInputStream in = new DataInputStream(new FileInputStream(filePath.toFile()))) {
            this.currentTerm = in.readLong();
            boolean hasVoted = in.readBoolean();
            if (hasVoted) {
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
    public void setCurrentTerm(long term) {
        this.currentTerm = term;
    }

    @Override
    public NodeAddress getVotedFor() {
        return votedFor;
    }

    @Override
    public void setVotedFor(NodeAddress address) {
        this.votedFor = address;
    }

    @Override
    public void flush() {
        File tempFile = new File(filePath.toString() + ".tmp");

        try (FileOutputStream fos = new FileOutputStream(tempFile);
             DataOutputStream out = new DataOutputStream(fos)) {

            out.writeLong(currentTerm);
            out.writeBoolean(votedFor != null);
            if (votedFor != null) {
                out.writeUTF(votedFor.host());
                out.writeInt(votedFor.port());
            }

            out.flush();
            fos.getFD().sync();

        } catch (IOException e) {
            log.error("Failed to flush metadata to disk", e);
        }

        tempFile.renameTo(filePath.toFile());
    }
}
