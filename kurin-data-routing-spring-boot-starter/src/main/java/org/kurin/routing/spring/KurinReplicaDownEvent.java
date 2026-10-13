package org.kurin.routing.spring;

import org.kurin.routing.DataSourceType;

/**
 * The replica of a shard is down: its reads go to the primary, or fail if so configured.
 */
public class KurinReplicaDownEvent extends KurinNodeStateEvent {
    public KurinReplicaDownEvent(Object source, String shard, String reason) {
        super(source, shard, DataSourceType.REPLICA, reason);
    }
}
