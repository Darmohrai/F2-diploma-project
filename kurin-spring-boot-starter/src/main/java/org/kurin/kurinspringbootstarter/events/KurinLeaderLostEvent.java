package org.kurin.kurinspringbootstarter.events;

import org.springframework.context.ApplicationEvent;

/**
 * This node stopped being the leader. Published asynchronously.
 */
public class KurinLeaderLostEvent extends ApplicationEvent {
    private final long term;
    private final String leaderId;
    private final String nodeId;

    public KurinLeaderLostEvent(Object source, long term, String leaderId, String nodeId) {
        super(source);
        this.term = term;
        this.leaderId = leaderId;
        this.nodeId = nodeId;
    }

    /**
     * @return the term this node is in after losing leadership
     */
    public long getTerm() {
        return term;
    }

    /**
     * @return id of the new leader if it is already known, otherwise null
     */
    public String getLeaderId() {
        return leaderId;
    }

    public String getNodeId() {
        return nodeId;
    }
}
