package org.kurin.network.dto;

public class ClusterMessage {
    private final byte type;
    private final long correlationId;
    private final Object payload;

    public ClusterMessage(byte type, long correlationId, Object payload) {
        this.type = type;
        this.correlationId = correlationId;
        this.payload = payload;
    }

    public byte getType() {
        return type;
    }

    public long getCorrelationId() {
        return correlationId;
    }

    public Object getPayload() {
        return payload;
    }
}
