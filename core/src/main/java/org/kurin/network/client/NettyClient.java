package org.kurin.network.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.kurin.api.exception.KurinRemoteException;
import org.kurin.network.context.NetworkContext;
import org.kurin.network.handler.KurinChannelInitializer;
import org.kurin.network.handler.NettyHandler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class NettyClient {
    private final Bootstrap bootstrap;
    private final NetworkContext context;

    public NettyClient(NetworkContext networkContext, EventLoopGroup sharedWorkerGroup) {
        this.context = networkContext;
        this.bootstrap = new Bootstrap();
        this.bootstrap.group(sharedWorkerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, networkContext.getNetworkConfig().getConnectTimeoutMs())
                .handler(new KurinChannelInitializer(networkContext, true));
    }

    /**
     * @return a future completed with the channel after the TCP connection and the Kurin handshake succeed
     */
    public CompletableFuture<Channel> connect(String host, int port) {
        CompletableFuture<Channel> promise = new CompletableFuture<>();

        ChannelFuture channelFuture = bootstrap.connect(host, port);
        channelFuture.addListener(future -> {
            if (!future.isSuccess()) {
                promise.completeExceptionally(future.cause());
                return;
            }
            Channel channel = channelFuture.channel();
            CompletableFuture<Channel> handshake = channel.attr(NettyHandler.HANDSHAKE).get();

            var handshakeTimeout = context.getTimer().newTimeout(timeout -> {
                if (handshake.completeExceptionally(new KurinRemoteException("Handshake with " + host + ":" + port + " timed out"))) {
                    channel.close();
                }
            }, context.getNetworkConfig().getConnectTimeoutMs(), TimeUnit.MILLISECONDS);

            handshake.whenComplete((ready, error) -> {
                handshakeTimeout.cancel();
                if (error != null) {
                    promise.completeExceptionally(error);
                } else {
                    promise.complete(ready);
                }
            });
        });

        return promise;
    }
}
