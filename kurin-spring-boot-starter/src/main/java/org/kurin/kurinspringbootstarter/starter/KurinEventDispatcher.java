package org.kurin.kurinspringbootstarter.starter;

import org.kurin.kurinspringbootstarter.events.KurinLeaderElectedEvent;
import org.kurin.kurinspringbootstarter.events.KurinLeaderLostEvent;
import org.kurin.raft.RaftStatus;
import org.kurin.raft.state.RoleChangeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

/**
 * Publishes leadership changes as application events on its own thread, in the order they happened.
 * Listeners may block; the Raft thread never waits for them.
 */
public class KurinEventDispatcher implements RoleChangeListener, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KurinEventDispatcher.class);

    private final ApplicationEventPublisher publisher;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(task -> {
        Thread thread = new Thread(task, "kurin-events");
        thread.setDaemon(true);
        return thread;
    });

    public KurinEventDispatcher(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public void onLeaderElected(RaftStatus status) {
        publish(new KurinLeaderElectedEvent(this, status.term(), status.leaderId(), status.nodeId()));
    }

    @Override
    public void onLeaderLost(RaftStatus status) {
        publish(new KurinLeaderLostEvent(this, status.term(), status.leaderId(), status.nodeId()));
    }

    private void publish(ApplicationEvent event) {
        try {
            executor.execute(() -> {
                try {
                    publisher.publishEvent(event);
                } catch (RuntimeException e) {
                    log.warn("A listener of {} failed", event.getClass().getSimpleName(), e);
                }
            });
        } catch (RejectedExecutionException e) {
            log.debug("{} dropped: the application context is closing", event.getClass().getSimpleName());
        }
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
