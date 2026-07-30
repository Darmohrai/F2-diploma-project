package org.kurin.network.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.kurin.network.context.NetworkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyServer {
    private static final Logger log = LoggerFactory.getLogger(NettyServer.class);

    private final NetworkContext context;
    private final int port;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture serverChannelFuture;

    public NettyServer(NetworkContext context, int port) {
        this.context = context;
        this.port = port;
    }

    public void start() {
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = context.getSharedWorkerGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ServerChannelInitializer(context));

            serverChannelFuture = bootstrap.bind(port).sync(); // todo make async
            log.info("Kurin Netty Server started on port {}", port);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Netty Server start was interrupted", e);
        }
    }

    public void shutdown() {
        log.info("Shutting down Kurin Netty Server...");
        try {
            if (serverChannelFuture != null) {
                serverChannelFuture.channel().close().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (bossGroup != null) bossGroup.shutdownGracefully();
            if (workerGroup != null) workerGroup.shutdownGracefully();
            log.info("Netty Server shutdown complete.");
        }
    }
}
