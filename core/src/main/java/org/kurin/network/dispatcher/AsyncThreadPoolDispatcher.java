package org.kurin.network.dispatcher;

import org.kurin.network.dto.ClusterMessage;
import org.kurin.network.mailbox.Mailbox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class AsyncThreadPoolDispatcher implements MessageDispatcher {

    private static final Logger log = LoggerFactory.getLogger(AsyncThreadPoolDispatcher.class);

    private final ExecutorService businessLogicPool = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 2
    );

    private final Mailbox nodeMailbox = new Mailbox(businessLogicPool);

    @Override
    public CompletableFuture<Object> dispatchAsync(ClusterMessage request) {
        CompletableFuture<Object> resultFuture = new CompletableFuture<>();

        nodeMailbox.submit(() -> {
            try {
                CompletableFuture<Object> logicFuture = processLogic(request);
                logicFuture.whenComplete((res, ex) -> {
                    if (ex != null) {
                        resultFuture.completeExceptionally(ex);
                    } else {
                        resultFuture.complete(res);
                    }
                });
            } catch (Exception e) {
                log.error("Business logic execution failed for request {}", request.getCorrelationId(), e);
                resultFuture.completeExceptionally(e);
            }
        });

        return resultFuture;
    }

    protected abstract CompletableFuture<Object> processLogic(ClusterMessage request);

    public void shutdown() {
        businessLogicPool.shutdown();
    }
}
