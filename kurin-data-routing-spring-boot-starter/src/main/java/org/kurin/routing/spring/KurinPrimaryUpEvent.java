package org.kurin.routing.spring;

import org.kurin.routing.DataSourceType;

/**
 * The primary of a shard is available again.
 */
public class KurinPrimaryUpEvent extends KurinNodeStateEvent {
    public KurinPrimaryUpEvent(Object source, String shard, String reason) {
        super(source, shard, DataSourceType.PRIMARY, reason);
    }
}
