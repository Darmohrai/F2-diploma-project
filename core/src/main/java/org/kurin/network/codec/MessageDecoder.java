package org.kurin.network.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.kurin.network.dto.ClusterMessage;
import org.kurin.network.dto.Hello;
import org.kurin.network.model.MessageType;
import org.kurin.network.serializer.KryoSerializer;

import java.io.IOException;
import java.util.List;

public class MessageDecoder extends MessageToMessageDecoder<ByteBuf> {
    public static final int HEADER_LENGTH = 15;
    public static final int LENGTH_FIELD_OFFSET = 11;

    private final short magicNumber;
    private final KryoSerializer serializer;

    public MessageDecoder(KryoSerializer serializer, short magicNumber) {
        this.serializer = serializer;
        this.magicNumber = magicNumber;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws IOException {
        // Broken framing cannot be recovered from: the exception closes the connection
        short magic = in.readShort();
        if (magic != magicNumber) {
            throw new CorruptedFrameException("Invalid magic number: " + magic);
        }
        byte type = in.readByte();
        long correlationId = in.readLong();
        int payloadLength = in.readInt();
        if (payloadLength != in.readableBytes()) {
            throw new CorruptedFrameException("Frame declares " + payloadLength + " payload bytes but carries " + in.readableBytes());
        }
        ByteBuf payloadBuffer = in.readSlice(payloadLength);

        try (ByteBufInputStream inputStream = new ByteBufInputStream(payloadBuffer)) {
            if (MessageType.isHandshake(type)) {
                out.add(new ClusterMessage(type, correlationId, Hello.readFrom(inputStream)));
                return;
            }
            Object payload;
            try {
                payload = serializer.deserialize(inputStream);
            } catch (RuntimeException e) {
                // The frame itself is intact, so the stream stays in sync: report the error to the peer instead
                out.add(ClusterMessage.undecodable(type, correlationId, e));
                return;
            }
            out.add(new ClusterMessage(type, correlationId, payload));
        }
    }
}
