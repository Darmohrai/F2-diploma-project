package org.kurin.network.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.kurin.api.exception.KurinSerializationException;
import org.kurin.network.dto.ClusterMessage;
import org.kurin.network.dto.Hello;
import org.kurin.network.model.MessageType;
import org.kurin.network.serializer.KryoSerializer;

import java.io.IOException;

public class MessageEncoder extends MessageToByteEncoder<ClusterMessage> {
    private final short magicNumber;
    private final int maxPayloadSize;
    private final KryoSerializer serializer;

    public MessageEncoder(KryoSerializer serializer, short magicNumber, int maxPayloadSize) {
        this.serializer = serializer;
        this.magicNumber = magicNumber;
        this.maxPayloadSize = maxPayloadSize;
    }

    // An exception thrown here fails the write promise and leaves the connection open
    @Override
    protected void encode(ChannelHandlerContext ctx, ClusterMessage message, ByteBuf out) throws IOException {
        out.writeShort(magicNumber);
        out.writeByte(message.getType());
        out.writeLong(message.getCorrelationId());

        int lengthIndex = out.writerIndex();
        out.writeInt(0);
        int payloadStartIndex = out.writerIndex();

        try (ByteBufOutputStream outputStream = new ByteBufOutputStream(out)) {
            if (MessageType.isHandshake(message.getType())) {
                ((Hello) message.getPayload()).writeTo(outputStream);
            } else {
                serializer.serialize(message.getPayload(), outputStream);
            }
        }

        int payloadLength = out.writerIndex() - payloadStartIndex;
        if (payloadLength > maxPayloadSize) {
            Object payload = message.getPayload();
            throw new KurinSerializationException("Payload of " + (payload != null ? payload.getClass().getSimpleName() : "null")
                    + " is " + payloadLength + " bytes, over the limit of " + maxPayloadSize + " bytes (maxPayloadSize)");
        }
        out.setInt(lengthIndex, payloadLength);
    }
}
