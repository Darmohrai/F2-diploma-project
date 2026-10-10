package org.kurin.kurinspringbootstarter.events;

import org.springframework.context.ApplicationEvent;

public class KurinLeaderElectedEvent extends ApplicationEvent {
    public KurinLeaderElectedEvent(Object source) {
        super(source);
    }
}
