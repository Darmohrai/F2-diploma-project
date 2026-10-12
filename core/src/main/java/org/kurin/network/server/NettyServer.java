package org.kurin.network.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.kurin.network.context.NetworkContext;
import org.kurin.network.handler.KurinChannelInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyServer {
    private static final Logger log = LoggerFactory.getLogger(NettyServer.class);

    private final NetworkContext context;
    private final int port;
    private final ChannelGroup acceptedChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private EventLoopGroup bossGroup;
    private Channel serverChannel;

    public NettyServer(NetworkContext context, int port) {
        this.context = context;
        this.port = port;
    }

    /**
     * @throws IllegalStateException if the port cannot be bound
     */
    public void start() {
        String bindHost = context.getNetworkConfig().getBindHost();
        this.bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("kurin-accept-" + port, true));
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, context.getSharedWorkerGroup())
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new KurinChannelInitializer(context, false) {
                    @Override
                    protected void initChannel(io.netty.channel.socket.SocketChannel ch) {
                        acceptedChannels.add(ch);
                        super.initChannel(ch);
                    }
                });
        try {
            serverChannel = bootstrap.bind(bindHost, port).sync().channel();
            log.info("Kurin node listening on {}:{}", bindHost, port);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            bossGroup.shutdownGracefully();
            throw new IllegalStateException("Interrupted while binding " + bindHost + ":" + port, e);
        } catch (Exception e) {
            bossGroup.shutdownGracefully();
            throw new IllegalStateException("Cannot bind Kurin node to " + bindHost + ":" + port + ": " + e.getMessage(), e);
        }
    }

    public void shutdown() {
        try {
            if (serverChannel != null) {
                serverChannel.close().sync();
            }
            acceptedChannels.close().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
            }
        }
    }
}
