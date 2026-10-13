package org.kurin.routing.spring;

import org.kurin.routing.DataSourceType;

/**
 * The replica of a shard is available again.
 */
public class KurinReplicaUpEvent extends KurinNodeStateEvent {
    public KurinReplicaUpEvent(Object source, String shard, String reason) {
        super(source, shard, DataSourceType.REPLICA, reason);
    }
}
