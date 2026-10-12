package org.kurin.network.context;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.kurin.network.client.ConnectionManager;
import org.kurin.network.client.KurinNetworkClient;
import org.kurin.network.client.NettyClient;
import org.kurin.network.config.NetworkConfig;
import org.kurin.network.dispatcher.MessageDispatcher;
import org.kurin.network.model.NodeAddress;
import org.kurin.network.serializer.KryoSerializer;
import org.kurin.network.tls.SslContexts;
import org.kurin.network.tracker.PendingRequestsTracker;

import java.util.concurrent.TimeUnit;

public class NetworkContext {
    private static final long TIMER_TICK_MS = 10;

    private final NodeAddress localAddress;
    private final NetworkConfig networkConfig;
    private final KryoSerializer kryoSerializer;
    private final PendingRequestsTracker tracker;
    private final MessageDispatcher dispatcher;
    private final EventLoopGroup sharedWorkerGroup;
    private final KurinNetworkClient client;
    private final Timer timer;
    private final SslContexts sslContexts;

    public NetworkContext(NodeAddress localAddress, MessageDispatcher dispatcher, NetworkConfig networkConfig,
                          KryoSerializer kryoSerializer) {
        this.localAddress = localAddress;
        this.networkConfig = networkConfig;
        this.dispatcher = dispatcher;
        this.kryoSerializer = kryoSerializer;
        this.sslContexts = networkConfig.getTls().isEnabled() ? SslContexts.create(networkConfig.getTls()) : null;
        this.tracker = new PendingRequestsTracker();
        this.sharedWorkerGroup = new NioEventLoopGroup(networkConfig.getWorkerThreads(),
                new DefaultThreadFactory("kurin-io-" + localAddress.port(), true));
        // A 10 ms tick keeps randomized election timeouts from collapsing into a few wheel buckets
        this.timer = new HashedWheelTimer(new DefaultThreadFactory("kurin-timer-" + localAddress.port(), true),
                TIMER_TICK_MS, TimeUnit.MILLISECONDS);

        NettyClient nettyClient = new NettyClient(this, this.sharedWorkerGroup);
        ConnectionManager connectionManager = new ConnectionManager(nettyClient, this.tracker, this.timer);
        this.client = new KurinNetworkClient(connectionManager, this.tracker, this.timer);
    }

    public void shutdown() {
        client.close();
        timer.stop();
        sharedWorkerGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly(3, TimeUnit.SECONDS);
    }

    public String getLocalNodeId() {
        return localAddress.asString();
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

    public SslContexts getSslContexts() {
        return sslContexts;
    }
}
