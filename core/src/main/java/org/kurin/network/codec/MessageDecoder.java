package org.kurin.network.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.kurin.network.dto.ClusterMessage;
import org.kurin.network.serializer.KryoSerializer;

import java.util.List;

public class MessageDecoder extends MessageToMessageDecoder<ByteBuf> {
    private final short magicNumber;
    private final KryoSerializer serializer;

    public MessageDecoder(KryoSerializer serializer, short magicNumber) {
        this.serializer = serializer;
        this.magicNumber = magicNumber;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {

        short magic = in.readShort();
        if (magic != magicNumber){
            throw new CorruptedFrameException("Invalid Magic Number: " + magic);
        }

        byte type = in.readByte();
        long correlationId = in.readLong();
        int payloadLength = in.readInt();

        byte[] payloadBytes = new byte[payloadLength];
        in.readBytes(payloadBytes);

        Object payload = serializer.deserialize(payloadBytes);

        out.add(new ClusterMessage(type, correlationId, payload));
    }
}
