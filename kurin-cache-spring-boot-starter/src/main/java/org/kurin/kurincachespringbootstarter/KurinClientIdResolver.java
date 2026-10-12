package org.kurin.kurincachespringbootstarter;

/**
 * Identifies the client of the current request for Session Sticky Routing.
 * Declare a bean of this type to replace the default strategy.
 */
@FunctionalInterface
public interface KurinClientIdResolver {

    /**
     * @return client id, or null if the current call cannot be attributed to a client (no stickiness is applied)
     */
    String resolveClientId();
}
