package org.kurin.kurinspringbootstarter.starter;

import org.kurin.serialization.KurinTypeRegistry;

/**
 * Registers serialized types that the framework cannot discover from handler signatures.
 * Beans of this type are called once, right before the node starts.
 */
@FunctionalInterface
public interface KurinTypeRegistrar {
    void registerTypes(KurinTypeRegistry registry);
}
