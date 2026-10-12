package org.kurin.network.handler;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import org.kurin.network.codec.MessageDecoder;
import org.kurin.network.codec.MessageEncoder;
import org.kurin.network.config.NetworkConfig;
import org.kurin.network.context.NetworkContext;

import java.util.concurrent.CompletableFuture;

public class KurinChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final NetworkContext context;
    private final boolean outbound;

    public KurinChannelInitializer(NetworkContext context, boolean outbound) {
        this.context = context;
        this.outbound = outbound;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        NetworkConfig config = context.getNetworkConfig();
        ChannelPipeline pipeline = ch.pipeline();

        if (context.getSslContexts() != null) {
            pipeline.addLast(outbound
                    ? context.getSslContexts().client().newHandler(ch.alloc())
                    : context.getSslContexts().server().newHandler(ch.alloc()));
        }
        if (outbound) {
            ch.attr(NettyHandler.HANDSHAKE).set(new CompletableFuture<>());
        }

        pipeline.addLast(new IdleStateHandler(0, 0, config.getIdleTimeoutSeconds()));
        pipeline.addLast(new LengthFieldBasedFrameDecoder(
                config.getMaxPayloadSize() + MessageDecoder.HEADER_LENGTH, MessageDecoder.LENGTH_FIELD_OFFSET, 4, 0, 0));
        pipeline.addLast(new MessageDecoder(context.getKryoSerializer(), config.getMagicNumber()));
        pipeline.addLast(new MessageEncoder(context.getKryoSerializer(), config.getMagicNumber(), config.getMaxPayloadSize()));
        pipeline.addLast(new NettyHandler(context, outbound));
    }
}
