package org.kurin.network.context;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
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

    private final Timer timer;

    public NetworkContext(MessageDispatcher dispatcher, NetworkConfig networkConfig) {
        this.networkConfig = networkConfig;

        this.dispatcher = dispatcher;
        this.kryoSerializer = new KryoSerializer(networkConfig.getRegisteredClasses());
        this.tracker = new PendingRequestsTracker();

        this.sharedWorkerGroup = new NioEventLoopGroup(networkConfig.getWorkerThreads());

        this.timer = new HashedWheelTimer();

        NettyClient nettyClient = new NettyClient(this, this.sharedWorkerGroup);
        ConnectionManager connectionManager = new ConnectionManager(nettyClient);
        this.client = new KurinNetworkClient(connectionManager, this.tracker, this.timer, networkConfig.getRequestTimeoutSeconds());
    }

    public void shutdown() {
        timer.stop();
        sharedWorkerGroup.shutdownGracefully();
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

    public Timer getTimer() {
        return timer;
    }

}
