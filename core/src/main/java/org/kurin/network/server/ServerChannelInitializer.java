package org.kurin.network.server;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import org.kurin.network.codec.MessageDecoder;
import org.kurin.network.codec.MessageEncoder;
import org.kurin.network.config.NetworkConfig;
import org.kurin.network.context.NetworkContext;
import org.kurin.network.handler.NettyHandler;

public class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final NetworkContext context;

    public ServerChannelInitializer(NetworkContext context) {
        this.context = context;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        NetworkConfig config = context.getNetworkConfig();
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast(new IdleStateHandler(0, 0, 15)); //todo not hardcode

        pipeline.addLast(new LengthFieldBasedFrameDecoder(config.getMaxPayloadSize(), 11, 4, 0, 0));

        pipeline.addLast(new MessageDecoder(context.getKryoSerializer(), config.getMagicNumber()));

        pipeline.addLast(new MessageEncoder(context.getKryoSerializer(), config.getMagicNumber()));

        pipeline.addLast(new NettyHandler(context.getTracker(), context.getDispatcher()));
    }
}
