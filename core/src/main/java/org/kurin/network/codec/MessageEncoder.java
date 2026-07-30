package org.kurin.network.codec;

import io.netty.buffer.ByteBuf;
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
        byte[] payloadBytes = serializer.serialize(message.getPayload());

        out.writeShort(magicNumber);
        out.writeByte(message.getType());
        out.writeLong(message.getCorrelationId());
        out.writeInt(payloadBytes.length);

        out.writeBytes(payloadBytes);
    }
}
