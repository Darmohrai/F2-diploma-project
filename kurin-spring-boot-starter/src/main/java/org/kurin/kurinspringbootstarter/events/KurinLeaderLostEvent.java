package org.kurin.kurinspringbootstarter.events;

import org.springframework.context.ApplicationEvent;

public class KurinLeaderLostEvent extends ApplicationEvent {
    public KurinLeaderLostEvent(Object source) {
        super(source);
    }
}
