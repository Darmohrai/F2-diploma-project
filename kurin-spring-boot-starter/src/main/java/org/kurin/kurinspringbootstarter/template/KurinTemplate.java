package org.kurin.kurinspringbootstarter.template;

import org.kurin.raft.KurinNode;

import java.util.concurrent.CompletableFuture;

public class KurinTemplate {

    private final KurinNode kurinNode;

    public KurinTemplate(KurinNode kurinNode) {
        this.kurinNode = kurinNode;
    }

    public <T> CompletableFuture<T> submitCommand(Object command) {
        return kurinNode.submitCommand(command);
    }

    public <T> CompletableFuture<T> submitQuery(Object query) {
        return kurinNode.submitQuery(query);
    }
}
