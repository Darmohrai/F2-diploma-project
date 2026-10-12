package org.kurin.network.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import org.kurin.api.exception.KurinRemoteException;
import org.kurin.api.exception.KurinSerializationException;
import org.kurin.network.config.NetworkConfig;
import org.kurin.network.context.NetworkContext;
import org.kurin.network.dto.ClusterMessage;
import org.kurin.network.dto.Hello;
import org.kurin.network.dto.RpcResult;
import org.kurin.network.model.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Per-connection handler: performs the handshake, then dispatches requests and completes responses.
 */
public class NettyHandler extends SimpleChannelInboundHandler<ClusterMessage> {
    private static final Logger log = LoggerFactory.getLogger(NettyHandler.class);

    /**
     * Completed with the channel once the handshake succeeds. Present on outbound connections only.
     */
    public static final AttributeKey<CompletableFuture<Channel>> HANDSHAKE = AttributeKey.valueOf("kurin.handshake");

    private final NetworkContext context;
    private final boolean outbound;
    private final String nonce = UUID.randomUUID().toString();
    private boolean authenticated;

    public NettyHandler(NetworkContext context, boolean outbound) {
        this.context = context;
        this.outbound = outbound;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (outbound) {
            ctx.writeAndFlush(new ClusterMessage(MessageType.HELLO, 0, localHello(nonce)));
        }
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        failHandshake(ctx, new KurinRemoteException("Connection to " + ctx.channel().remoteAddress() + " closed during handshake"));
        ctx.fireChannelInactive();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            log.debug("Connection {} is idle. Closing.", ctx.channel().remoteAddress());
            ctx.close();
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ClusterMessage msg) {
        switch (msg.getType()) {
            case MessageType.HELLO -> onHello(ctx, (Hello) msg.getPayload());
            case MessageType.HELLO_ACK -> onHelloAck(ctx, (Hello) msg.getPayload());
            case MessageType.HELLO_REJECT -> onHelloReject(ctx, (Hello) msg.getPayload());
            case MessageType.REQUEST -> onRequest(ctx, msg);
            case MessageType.RESPONSE -> onResponse(msg);
            default -> {
                log.warn("Unknown message type {} from {}. Closing connection.", msg.getType(), ctx.channel().remoteAddress());
                ctx.close();
            }
        }
    }

    private void onHello(ChannelHandlerContext ctx, Hello remote) {
        String problem = outbound ? "unexpected HELLO on an outbound connection" : validate(remote, remote.nonce());
        if (problem != null) {
            log.error("Rejected connection from node {} ({}): {}", remote.nodeId(), ctx.channel().remoteAddress(), problem);
            NetworkConfig config = context.getNetworkConfig();
            Hello reject = Hello.reject(config.getClusterName(), context.getLocalNodeId(), NetworkConfig.PROTOCOL_VERSION, problem);
            ctx.writeAndFlush(new ClusterMessage(MessageType.HELLO_REJECT, 0, reject)).addListener(ChannelFutureListener.CLOSE);
            return;
        }
        authenticated = true;
        ctx.writeAndFlush(new ClusterMessage(MessageType.HELLO_ACK, 0, localHello(remote.nonce())));
    }

    private void onHelloAck(ChannelHandlerContext ctx, Hello remote) {
        String problem = !outbound ? "unexpected HELLO_ACK on an inbound connection"
                : !nonce.equals(remote.nonce()) ? "handshake nonce was not echoed"
                : validate(remote, nonce);
        if (problem != null) {
            log.error("Refusing to talk to node {} ({}): {}", remote.nodeId(), ctx.channel().remoteAddress(), problem);
            failHandshake(ctx, new KurinRemoteException("Handshake with " + remote.nodeId() + " failed: " + problem));
            ctx.close();
            return;
        }
        authenticated = true;
        CompletableFuture<Channel> handshake = ctx.channel().attr(HANDSHAKE).get();
        if (handshake != null) {
            handshake.complete(ctx.channel());
        }
    }

    private void onHelloReject(ChannelHandlerContext ctx, Hello remote) {
        log.error("Node {} ({}) rejected the connection: {}", remote.nodeId(), ctx.channel().remoteAddress(), remote.rejectReason());
        failHandshake(ctx, new KurinRemoteException("Node " + remote.nodeId() + " rejected the connection: " + remote.rejectReason()));
        ctx.close();
    }

    private void onRequest(ChannelHandlerContext ctx, ClusterMessage msg) {
        if (!authenticated) {
            log.warn("Request from {} before the handshake completed. Closing connection.", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }
        if (msg.getDecodeError() != null) {
            log.warn("Cannot decode request {} from {}: {}", msg.getCorrelationId(), ctx.channel().remoteAddress(),
                    msg.getDecodeError().getMessage());
            respond(ctx, msg.getCorrelationId(), RpcResult.failure(msg.getDecodeError()));
            return;
        }
        CompletableFuture<Object> result;
        try {
            result = context.getDispatcher().dispatchAsync(msg);
        } catch (RuntimeException e) {
            result = CompletableFuture.failedFuture(e);
        }
        result.whenComplete((value, error) ->
                respond(ctx, msg.getCorrelationId(), error != null ? RpcResult.failure(error) : RpcResult.success(value)));
    }

    // A request is always answered: if the result itself cannot be written, the error is sent instead
    private void respond(ChannelHandlerContext ctx, long correlationId, RpcResult result) {
        ctx.writeAndFlush(new ClusterMessage(MessageType.RESPONSE, correlationId, result)).addListener(future -> {
            if (future.isSuccess() || !ctx.channel().isActive()) {
                return;
            }
            Throwable cause = RpcResult.unwrap(future.cause());
            if (result.ok()) {
                log.warn("Cannot send the result of request {}: {}", correlationId, cause.getMessage());
                respond(ctx, correlationId, RpcResult.failure(
                        new KurinSerializationException("Result could not be sent: " + cause.getMessage())));
            } else {
                log.error("Cannot send the error response of request {}: {}", correlationId, cause.getMessage());
            }
        });
    }

    private void onResponse(ClusterMessage msg) {
        if (msg.getDecodeError() != null) {
            context.getTracker().fail(msg.getCorrelationId(),
                    new KurinSerializationException("Cannot decode response: " + msg.getDecodeError().getMessage(), msg.getDecodeError()));
        } else if (msg.getPayload() instanceof RpcResult result) {
            context.getTracker().complete(msg.getCorrelationId(), result);
        } else {
            context.getTracker().fail(msg.getCorrelationId(), new KurinRemoteException("Malformed response payload"));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Closing connection {} after a network error: {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }

    private void failHandshake(ChannelHandlerContext ctx, Throwable reason) {
        CompletableFuture<Channel> handshake = ctx.channel().attr(HANDSHAKE).get();
        if (handshake != null) {
            handshake.completeExceptionally(reason);
        }
    }

    private Hello localHello(String nonceToSign) {
        NetworkConfig config = context.getNetworkConfig();
        String nodeId = context.getLocalNodeId();
        return new Hello(config.getClusterName(), nodeId, NetworkConfig.PROTOCOL_VERSION,
                context.getKryoSerializer().registry().fingerprint(), nonceToSign,
                hmac(config.getClusterSecret(), config.getClusterName(), nodeId, nonceToSign), null);
    }

    /**
     * @return a description of the mismatch, or null if the remote node may join the conversation
     */
    private String validate(Hello remote, String expectedNonce) {
        NetworkConfig config = context.getNetworkConfig();
        if (!config.getClusterName().equals(remote.clusterName())) {
            return "cluster name mismatch: local '" + config.getClusterName() + "', remote '" + remote.clusterName() + "'";
        }
        if (remote.protocolVersion() != NetworkConfig.PROTOCOL_VERSION) {
            return "protocol version mismatch: local " + NetworkConfig.PROTOCOL_VERSION + ", remote " + remote.protocolVersion();
        }
        String localFingerprint = context.getKryoSerializer().registry().fingerprint();
        if (!localFingerprint.equals(remote.typeRegistryFingerprint())) {
            return "type registry mismatch: local fingerprint " + localFingerprint + ", remote " + remote.typeRegistryFingerprint()
                    + ". All nodes must register the same set of command, query and result types.";
        }
        String expectedHmac = hmac(config.getClusterSecret(), remote.clusterName(), remote.nodeId(), expectedNonce);
        if (!MessageDigest.isEqual(expectedHmac.getBytes(StandardCharsets.UTF_8), remote.hmac().getBytes(StandardCharsets.UTF_8))) {
            return "cluster secret mismatch (HMAC verification failed)";
        }
        return null;
    }

    private static String hmac(String secret, String clusterName, String nodeId, String nonce) {
        if (secret == null || secret.isEmpty()) {
            return "";
        }
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal((clusterName + "|" + nodeId + "|" + nonce).getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 is not available", e);
        }
    }
}
