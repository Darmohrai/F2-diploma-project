package org.kurin.kurinspringbootstarter.events;

import org.springframework.context.ApplicationEvent;

/**
 * This node became the leader. Published asynchronously: by the time a listener runs, leadership may
 * already be lost again. Code that must run only on the leader should check {@code KurinTemplate.leadership()}.
 */
public class KurinLeaderElectedEvent extends ApplicationEvent {
    private final long term;
    private final String leaderId;
    private final String nodeId;

    public KurinLeaderElectedEvent(Object source, long term, String leaderId, String nodeId) {
        super(source);
        this.term = term;
        this.leaderId = leaderId;
        this.nodeId = nodeId;
    }

    public long getTerm() {
        return term;
    }

    /**
     * @return id of the leader, which is this node
     */
    public String getLeaderId() {
        return leaderId;
    }

    public String getNodeId() {
        return nodeId;
    }
}
