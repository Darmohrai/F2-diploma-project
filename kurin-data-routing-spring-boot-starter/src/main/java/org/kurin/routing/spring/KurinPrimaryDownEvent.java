package org.kurin.routing.spring;

import org.kurin.routing.DataSourceType;

/**
 * The primary of a shard is down: writes and strict reads of the shard fail fast, eventual reads continue on the replica.
 */
public class KurinPrimaryDownEvent extends KurinNodeStateEvent {
    public KurinPrimaryDownEvent(Object source, String shard, String reason) {
        super(source, shard, DataSourceType.PRIMARY, reason);
    }
}
