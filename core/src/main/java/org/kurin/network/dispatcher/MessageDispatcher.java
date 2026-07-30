package org.kurin.network.dispatcher;

import org.kurin.network.dto.ClusterMessage;

public interface MessageDispatcher {
    Object dispatch(ClusterMessage request);
}
