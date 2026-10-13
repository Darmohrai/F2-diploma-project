package org.kurin.routing.spring;

import org.kurin.routing.DataSourceType;
import org.springframework.context.ApplicationEvent;

/**
 * A database node became unavailable or available again. Concrete events:
 * {@link KurinPrimaryDownEvent}, {@link KurinPrimaryUpEvent}, {@link KurinReplicaDownEvent}, {@link KurinReplicaUpEvent}.
 */
public abstract class KurinNodeStateEvent extends ApplicationEvent {
    private final String shard;
    private final DataSourceType role;
    private final String reason;

    protected KurinNodeStateEvent(Object source, String shard, DataSourceType role, String reason) {
        super(source);
        this.shard = shard;
        this.role = role;
        this.reason = reason;
    }

    public String getShard() {
        return shard;
    }

    public DataSourceType getRole() {
        return role;
    }

    /**
     * @return what was observed, for example the error of the failed connection attempt
     */
    public String getReason() {
        return reason;
    }

}
