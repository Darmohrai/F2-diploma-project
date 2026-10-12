package org.kurin.network.dto;

public class ClusterMessage {
    private final byte type;
    private final long correlationId;
    private final Object payload;
    private final Throwable decodeError;

    public ClusterMessage(byte type, long correlationId, Object payload) {
        this(type, correlationId, payload, null);
    }

    private ClusterMessage(byte type, long correlationId, Object payload, Throwable decodeError) {
        this.type = type;
        this.correlationId = correlationId;
        this.payload = payload;
        this.decodeError = decodeError;
    }

    /**
     * A well-framed message whose payload could not be deserialized.
     */
    public static ClusterMessage undecodable(byte type, long correlationId, Throwable error) {
        return new ClusterMessage(type, correlationId, null, error);
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

    /**
     * @return the deserialization failure, or null if the payload was decoded
     */
    public Throwable getDecodeError() {
        return decodeError;
    }
}
