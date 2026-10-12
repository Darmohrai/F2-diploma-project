package org.kurin.kurinspringbootstarter.starter;

import org.kurin.raft.KurinNode;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;

import java.util.List;

/**
 * Starts the node once every singleton exists, and before the web server opens its port.
 */
public class KurinNodeLifecycle implements SmartLifecycle {
    /**
     * Earlier than the embedded web server (which starts near the end of the phases): the application
     * joins the cluster before it accepts requests, and leaves it after it stopped accepting them.
     */
    public static final int PHASE = 0;

    private final KurinNode node;
    private final KurinServiceRegistry registry;
    private final KurinSerializationProperties serialization;
    private final ObjectProvider<KurinTypeRegistrar> typeRegistrars;
    private final ObjectProvider<KurinNodeCustomizer> customizers;
    private volatile boolean running;

    public KurinNodeLifecycle(KurinNode node, KurinServiceRegistry registry, KurinSerializationProperties serialization,
                              ObjectProvider<KurinTypeRegistrar> typeRegistrars, ObjectProvider<KurinNodeCustomizer> customizers) {
        this.node = node;
        this.registry = registry;
        this.serialization = serialization;
        this.typeRegistrars = typeRegistrars;
        this.customizers = customizers;
    }

    /**
     * Any failure here (invalid handler signature, conflicting type ids, unusable data directory, port in use)
     * propagates and fails the startup of the application context.
     */
    @Override
    public void start() {
        registry.freeze();
        List<KurinServiceRegistry.RegisteredService> services = registry.services();
        services.forEach(service -> node.addService(service.target(), service.type()));

        serialization.getTypes().forEach(node::registerType);
        typeRegistrars.orderedStream().forEach(registrar -> registrar.registerTypes(node.typeRegistry()));

        List<Object> serviceBeans = services.stream().map(KurinServiceRegistry.RegisteredService::target).toList();
        customizers.orderedStream().forEach(customizer -> customizer.customize(node, serviceBeans));

        node.start();
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        node.shutdown();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
