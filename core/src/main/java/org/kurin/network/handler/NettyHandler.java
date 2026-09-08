package org.kurin.network.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;
import org.kurin.network.dispatcher.MessageDispatcher;
import org.kurin.network.dto.ClusterMessage;
import org.kurin.network.model.MessageType;
import org.kurin.network.tracker.PendingRequestsTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyHandler extends SimpleChannelInboundHandler<ClusterMessage> {

    private static final Logger log = LoggerFactory.getLogger(NettyHandler.class);

    private final PendingRequestsTracker tracker;
    private final MessageDispatcher dispatcher;

    public NettyHandler(PendingRequestsTracker tracker, MessageDispatcher dispatcher) {
        this.tracker = tracker;
        this.dispatcher = dispatcher;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            log.warn("Connection idle detected. Closing channel {}", ctx.channel().remoteAddress());
            ctx.close();
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ClusterMessage msg) {
        if (msg.getType() == MessageType.RESPONSE) {
            tracker.completeRequest(msg.getCorrelationId(), msg);
        } else if (msg.getType() == MessageType.REQUEST) {
            dispatcher.dispatchAsync(msg).whenComplete((resultPayload, throwable) -> {
                if (throwable != null) {
                    log.error("Error executing business logic for request {}: {}",
                            msg.getCorrelationId(), throwable.getMessage());
                    return;
                }
                if (resultPayload != null) {
                    ClusterMessage response = new ClusterMessage(
                            MessageType.RESPONSE,
                            msg.getCorrelationId(),
                            resultPayload
                    );
                    ctx.writeAndFlush(response);
                }
            });

        } else if (msg.getType() == MessageType.PING) {
            ctx.writeAndFlush(new ClusterMessage(MessageType.RESPONSE, msg.getCorrelationId(), "PONG"));
        } else {
            log.warn("Received unknown message type: {}", msg.getType());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Network error occurred in Netty pipeline: {}", cause.getMessage());
        ctx.close();
    }
}
