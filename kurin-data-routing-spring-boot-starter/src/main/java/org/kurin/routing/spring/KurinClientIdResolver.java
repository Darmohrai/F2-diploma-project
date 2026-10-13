package org.kurin.routing.spring;

/**
 * Identifies the client of the current request for the LOCAL_SESSION consistency strategy.
 * Declare a bean of this type to replace the default.
 */
@FunctionalInterface
public interface KurinClientIdResolver {

    /**
     * @return client id, or null if the current call cannot be attributed to a client (no stickiness is applied)
     */
    String resolveClientId();
}
