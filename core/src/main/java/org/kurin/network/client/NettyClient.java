package org.kurin.network.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.kurin.network.context.NetworkContext;

import java.util.concurrent.CompletableFuture;

public class NettyClient {

    private final Bootstrap bootstrap;

    public NettyClient(NetworkContext networkContext, EventLoopGroup sharedWorkerGroup) {
        this.bootstrap = new Bootstrap();

        this.bootstrap.group(sharedWorkerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ClientChannelInitializer(networkContext));
    }

    public CompletableFuture<Channel> connect(String host, int port) {
        CompletableFuture<Channel> promise = new CompletableFuture<>();

        ChannelFuture channelFuture = bootstrap.connect(host, port);

        channelFuture.addListener(future -> {
            if (future.isSuccess()) {
                promise.complete(channelFuture.channel());
            } else {
                promise.completeExceptionally(future.cause());
            }
        });

        return promise;
    }
}
