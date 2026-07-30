package org.kurin.network.context;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.kurin.network.client.ConnectionManager;
import org.kurin.network.client.KurinNetworkClient;
import org.kurin.network.client.NettyClient;
import org.kurin.network.config.NetworkConfig;
import org.kurin.network.dispatcher.MessageDispatcher;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.network.tracker.PendingRequestsTracker;

public class NetworkContext {

    private final NetworkConfig networkConfig;

    private final KryoSerializer kryoSerializer;
    private final PendingRequestsTracker tracker;
    private final MessageDispatcher dispatcher;
    private final EventLoopGroup sharedWorkerGroup;
    private final KurinNetworkClient client;

    public NetworkContext(MessageDispatcher dispatcher, NetworkConfig networkConfig) {
        this.networkConfig = networkConfig;

        this.dispatcher = dispatcher;
        this.kryoSerializer = new KryoSerializer();
        this.tracker = new PendingRequestsTracker();

        this.sharedWorkerGroup = new NioEventLoopGroup(networkConfig.getWorkerThreads());

        NettyClient nettyClient = new NettyClient(this, this.sharedWorkerGroup);
        ConnectionManager connectionManager = new ConnectionManager(nettyClient);
        this.client = new KurinNetworkClient(connectionManager, this.tracker, networkConfig.getRequestTimeoutSeconds());
    }

    public NetworkConfig getNetworkConfig() {
        return networkConfig;
    }

    public KryoSerializer getKryoSerializer() {
        return kryoSerializer;
    }

    public PendingRequestsTracker getTracker() {
        return tracker;
    }

    public MessageDispatcher getDispatcher() {
        return dispatcher;
    }

    public EventLoopGroup getSharedWorkerGroup() {
        return sharedWorkerGroup;
    }

    public KurinNetworkClient getClient() {
        return client;
    }
}
