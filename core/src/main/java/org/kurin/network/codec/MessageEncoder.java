package org.kurin.network.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.kurin.network.dto.ClusterMessage;
import org.kurin.network.serializer.KryoSerializer;

public class MessageEncoder extends MessageToByteEncoder<ClusterMessage> {
    private final short magicNumber;
    private final KryoSerializer serializer;

    public MessageEncoder(KryoSerializer serializer, short magicNumber) {
        this.serializer = serializer;
        this.magicNumber = magicNumber;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ClusterMessage message, ByteBuf out) {
        out.writeShort(magicNumber);
        out.writeByte(message.getType());
        out.writeLong(message.getCorrelationId());

        int lengthIndex = out.writerIndex();
        out.writeInt(0);

        int payloadStartIndex = out.writerIndex();

        try (ByteBufOutputStream outputStream = new ByteBufOutputStream(out)) {
            serializer.serialize(message.getPayload(), outputStream);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize payload", e);
        }

        int payloadLength = out.writerIndex() - payloadStartIndex;
        out.setInt(lengthIndex, payloadLength);
    }
}
