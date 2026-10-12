package org.kurin.kurinspringbootstarter.starter;

import org.kurin.raft.KurinNode;

/**
 * Adjusts the configuration of the node after the {@code kurin.node.*} properties were applied
 * and before the node object is created.
 */
@FunctionalInterface
public interface KurinNodeBuilderCustomizer {
    void customize(KurinNode.Builder builder);
}
