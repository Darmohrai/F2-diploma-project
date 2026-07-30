package org.kurin.network.client;

import io.netty.channel.Channel;
import org.kurin.network.model.NodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionManager {
    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    private final NettyClient nettyClient;
    private final Map<NodeAddress, CompletableFuture<Channel>> activeConnections = new ConcurrentHashMap<>();

    public ConnectionManager(NettyClient nettyClient) {
        this.nettyClient = nettyClient;
    }

    public CompletableFuture<Channel> getOrConnect(NodeAddress address) {
        return activeConnections.computeIfAbsent(address, addr -> {
            log.info("Creating new connection to {}", addr.asString());
            return nettyClient.connect(addr.host(), addr.port());
        }).handle(((channel, throwable) -> {
            if (throwable != null || !channel.isActive()) {
                log.warn("Connection to {} failed or dead. Removing from cache.", address.asString());
                activeConnections.remove(address);

                throw new RuntimeException("Cannot connect to " + address.asString(), throwable);
            }
            return channel;
        }));
    }

    public void sendAsync(NodeAddress destination, Object message) {
        getOrConnect(destination).thenAccept(channel -> {
            channel.writeAndFlush(message);
        });
    }
}
