package org.kurin.network.dto;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Handshake message. Encoded by hand rather than with Kryo, so that nodes with different type registries
 * can still tell each other why they refuse to connect.
 *
 * @param nonce        random value chosen by the connecting side and echoed by the accepting side
 * @param hmac         HMAC-SHA256 over {@code clusterName|nodeId|nonce}, empty when no cluster secret is set
 * @param rejectReason set only in a HELLO_REJECT message
 */
public record Hello(String clusterName, String nodeId, int protocolVersion, String typeRegistryFingerprint,
                    String nonce, String hmac, String rejectReason) {

    public static Hello reject(String clusterName, String nodeId, int protocolVersion, String reason) {
        return new Hello(clusterName, nodeId, protocolVersion, "", "", "", reason);
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(clusterName);
        out.writeUTF(nodeId);
        out.writeInt(protocolVersion);
        out.writeUTF(typeRegistryFingerprint);
        out.writeUTF(nonce);
        out.writeUTF(hmac);
        out.writeUTF(rejectReason != null ? rejectReason : "");
    }

    public static Hello readFrom(DataInput in) throws IOException {
        return new Hello(in.readUTF(), in.readUTF(), in.readInt(), in.readUTF(), in.readUTF(), in.readUTF(), in.readUTF());
    }
}
