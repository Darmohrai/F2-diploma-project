package org.kurin.kurinspringbootstarter.starter;

import org.kurin.raft.KurinNode;

import java.util.List;

/**
 * Called once right before the node starts, when every state machine service of the application is known.
 * The node still accepts listeners, services and types at this point.
 */
@FunctionalInterface
public interface KurinNodeCustomizer {

    /**
     * @param services the beans registered as state machine services
     */
    void customize(KurinNode node, List<Object> services);
}
