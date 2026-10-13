package org.kurin.routing.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.event.SmartApplicationListener;

/**
 * Knows whether the application of this context is still starting. The answer belongs to the context, not to the
 * JVM: several applications in one JVM, as in tests, each have their own.
 * <p>
 * The start is over when Spring Boot publishes {@link ApplicationReadyEvent} for this context: after the
 * context is refreshed and the application runners have run. A context that is not started by
 * {@code SpringApplication} never gets that event; call {@link #finish()} there.
 */
public class KurinRoutingStartup implements SmartApplicationListener, ApplicationContextAware {
    private static final Logger log = LoggerFactory.getLogger(KurinRoutingStartup.class);

    private ApplicationContext context;
    private volatile boolean finished;

    @Override
    public void setApplicationContext(ApplicationContext context) {
        this.context = context;
    }

    @Override
    public boolean supportsEventType(Class<? extends ApplicationEvent> eventType) {
        return ApplicationReadyEvent.class.isAssignableFrom(eventType);
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        // Events of a child context are also delivered to its parent
        if (event instanceof ApplicationReadyEvent ready && ready.getApplicationContext() == context) {
            finish();
        }
    }

    /**
     * Declares the start of the application over.
     */
    public void finish() {
        if (!finished) {
            finished = true;
            log.debug("The application is ready: from now on database access needs a shard");
        }
    }

    public boolean isFinished() {
        return finished;
    }
}
