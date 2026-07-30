package org.kurin.network;

import org.kurin.network.client.KurinNetworkClient;
import org.kurin.network.config.NetworkConfig;
import org.kurin.network.context.NetworkContext;
import org.kurin.network.dispatcher.MessageDispatcher;
import org.kurin.network.server.NettyServer;

public class NetworkStarter {

    private final NetworkContext context;
    private final NettyServer server;

    public NetworkStarter(int port, MessageDispatcher dispatcher, NetworkConfig networkConfig) {
        this.context = new NetworkContext(dispatcher, networkConfig);
        this.server = new NettyServer(this.context, port);
    }

    public void start() {
        server.start();
    }

    public void shutdown() {
        server.shutdown();
        context.getSharedWorkerGroup().shutdownGracefully();
    }

    public KurinNetworkClient getClient() {
        return context.getClient();
    }
}
