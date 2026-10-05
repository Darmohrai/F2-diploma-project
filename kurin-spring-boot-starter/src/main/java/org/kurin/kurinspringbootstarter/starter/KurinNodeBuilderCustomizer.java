package org.kurin.kurinspringbootstarter.starter;

import org.kurin.raft.KurinNode;

import java.util.List;

public interface KurinNodeBuilderCustomizer {
    void customize(KurinNode.Builder builder, List<Object> registeredServices);
}
