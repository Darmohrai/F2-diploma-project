package org.kurin.network;

import io.netty.util.Timer;
import org.kurin.network.client.KurinNetworkClient;
import org.kurin.network.config.NetworkConfig;
import org.kurin.network.context.NetworkContext;
import org.kurin.network.dispatcher.MessageDispatcher;
import org.kurin.network.model.NodeAddress;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.network.server.NettyServer;

public class NetworkStarter {

    private final NetworkContext context;
    private final NettyServer server;

    public NetworkStarter(NodeAddress localAddress, MessageDispatcher dispatcher, NetworkConfig networkConfig,
                          KryoSerializer serializer) {
        this.context = new NetworkContext(localAddress, dispatcher, networkConfig, serializer);
        this.server = new NettyServer(this.context, localAddress.port());
    }

    public void start() {
        server.start();
    }

    public void shutdown() {
        server.shutdown();
        context.shutdown();
    }

    public KurinNetworkClient getClient() {
        return context.getClient();
    }

    public Timer getTimer() {
        return context.getTimer();
    }
}
